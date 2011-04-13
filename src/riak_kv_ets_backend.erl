%% -------------------------------------------------------------------
%%
%% riak_ets_backend: storage engine based on ETS tables
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

% @doc riak_kv_ets_backend is a Riak storage backend using ets.

-module(riak_kv_ets_backend).
-behavior(riak_kv_backend).
-behavior(gen_server).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-export([capability/0,capability/2,
         capability/1,capability/3, %% TODO delete these two
         %% TODO: Verify that list/1 is either deprecated or maintained
         %%       for debugging/testing purposes.  It seems to me that
         %%       the vnode interface isn't used by riak_kv_vnode at all.
         start/2,stop/1,get/2,put/3,list/1,list_bucket/2,delete/2,
         is_empty/1, drop/1, fold/3, fold_bucket_keys/4, callback/3]).
%% Backend version 2 API
-export([bev2_new_bucket_iterator/2, bev2_new_fold_iterator/6,
         bev2_iterate/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% @type state() = term().
-record(state, {
          t         :: ets:tab(),
          ordered_p :: boolean()
         }).

-spec capability() -> [term()].

capability() ->
    [%% Mandatory
     {api_version, 2},
     %% Advisory
     {has_ordered_keys, maybe},
     {fold_will_block, false},
     {list_will_block, false},
     %% Perhaps helpful hints
     {keys_and_values_stored_together, true},
     {vclocks_and_values_stored_together, true}].

-spec capability(term(), 'undefined' | binary()) -> [term()].

capability(SrvRef, Bucket) ->
    gen_server:call(SrvRef, {capability, Bucket}, infinity).

%% TODO: delete to DELETEME
-spec capability(atom()) -> boolean() | 'maybe'.
capability(has_ordered_keys) ->
    maybe;
capability(keys_and_values_stored_together) ->
    true;
capability(vclocks_and_values_stored_together) ->
    true;
capability(fold_will_block) ->
    true; %% SLF TODO: change this
capability(_) ->
    false.
-spec capability(term(), binary(), atom()) -> boolean().
capability(_State, _Bucket, has_ordered_keys) ->
    false; %% SLF TODO: if table is ordered_set, then true!
capability(_State, _Bucket, keys_and_values_stored_together) ->
    true;
capability(_State, _Bucket, vclocks_and_values_stored_together) ->
    true;
capability(_State, _Bucket, fold_will_block) ->
    true; %% SLF TODO: change this
capability(_State, _Bucket, _) ->
    false.
%%% TODO: DELETEME end

% @spec start(Partition :: integer(), Config :: proplist()) ->
%                        {ok, state()} | {{error, Reason :: term()}, state()}
start(Partition, Config) ->
    TableType = proplists:get_value(ets_backend_table_type, Config, set),
    gen_server:start_link(?MODULE, [Partition, TableType], []).

%% @private
init([Partition, TableType]) ->
    {ok, #state{t=ets:new(list_to_atom(integer_to_list(Partition)),
                          [TableType]),
                ordered_p = (TableType == ordered_set)}}.

%% @private
handle_cast(_, State) -> {noreply, State}.

%% @private
handle_call(stop,_From,State) -> {reply, srv_stop(State), State};
handle_call({get,BKey},_From,State) -> {reply, srv_get(State,BKey), State};
handle_call({put,BKey,Val},_From,State) ->
    {reply, srv_put(State,BKey,Val),State};
handle_call({delete,BKey},_From,State) -> {reply, srv_delete(State,BKey),State};
handle_call(list,_From,State) -> {reply, srv_list(State), State};
handle_call({list_bucket,Bucket},_From,State) ->
    {reply, srv_list_bucket(State, Bucket), State};
handle_call(is_empty, _From, State) ->
    {reply, ets:info(State#state.t, size) =:= 0, State};
handle_call(drop, _From, State) -> 
    ets:delete(State#state.t),
    {reply, ok, State};
handle_call({fold, Fun0, Acc}, _From, State) ->
    Fun = fun({{B,K}, V}, AccIn) -> Fun0({B,K}, V, AccIn) end,
    Reply = ets:foldl(Fun, Acc, State#state.t),
    {reply, Reply, State};
handle_call({fold_bucket_keys, _Bucket, Fun0, Acc}, From, State) ->
    %% We could do something with the Bucket arg, but for this backend
    %% there isn't much point, so we'll do the same thing as the older
    %% API fold.
    handle_call({fold, Fun0, Acc}, From, State);
handle_call({capability, _Bucket}, _From, State) ->
    Reply = [{has_ordered_keys, State#state.ordered_p}|capability()],
    {reply, Reply, State};
handle_call({bev2_new_bucket_iterator, _Idx}, _From, State) ->
    Reply = {bucket_iterator, fun() -> ets:first(State#state.t) end},
    {reply, Reply, State};
handle_call({bev2_new_fold_iterator, Idx, Bucket, WantBKey, WantMd, WantVal},
            _From, State) ->
    Reply = make_bev2_fold_iterator(Idx, Bucket, WantBKey, WantMd, WantVal, State),
    {reply, Reply, State};
handle_call({bev2_iterate, Iter}, _From, State) ->
    Reply = do_bev2_iterate(Iter, State),
    {reply, Reply, State}.

% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(SrvRef) -> gen_server:call(SrvRef,stop).
srv_stop(State) ->
    catch ets:delete(State#state.t),
    ok.

% get(state(), riak_object:bkey()) ->
%   {ok, Val :: binary()} | {error, Reason :: term()}
% key must be 160b
get(SrvRef, BKey) -> gen_server:call(SrvRef,{get,BKey}).
srv_get(State, BKey) ->
    case ets:lookup(State#state.t,BKey) of
        [] -> {error, notfound};
        [{BKey,Val}] -> {ok, Val};
        Err -> {error, Err}
    end.

% put(state(), riak_object:bkey(), Val :: binary()) ->
%   ok | {error, Reason :: term()}
% key must be 160b
put(SrvRef, BKey, Val) -> gen_server:call(SrvRef,{put,BKey,Val}).
srv_put(State,BKey,Val) ->
    true = ets:insert(State#state.t, {BKey,Val}),
    ok.

% delete(state(), riak_object:bkey()) ->
%   ok | {error, Reason :: term()}
% key must be 160b
delete(SrvRef, BKey) -> gen_server:call(SrvRef,{delete,BKey}).
srv_delete(State, BKey) ->
    true = ets:delete(State#state.t, BKey),
    ok.

%% SLF TODO: I don't believe that this function is ever used by the vnode,
%%           so to heck with mangling it to the new way of doing things.
% list(state()) -> [riak_object:bkey()]
list(SrvRef) -> gen_server:call(SrvRef,list).
srv_list(State) ->
    MList = ets:match(State#state.t,{'$1','_'}),
    list(MList,[]).
list([],Acc) -> Acc;
list([[K]|Rest],Acc) -> list(Rest,[K|Acc]).

% list_bucket(term(), Bucket :: riak_object:bucket()) -> [Key :: binary()]
list_bucket(SrvRef, Bucket) ->
    gen_server:call(SrvRef,{list_bucket, Bucket}).
srv_list_bucket(State, {filter, Bucket, Fun}) ->
    MList = lists:filter(Fun, ets:match(State#state.t,{{Bucket,'$1'},'_'})),
    list(MList,[]);
srv_list_bucket(State, Bucket) ->
    case Bucket of
        '_' -> MatchSpec = {{'$1','_'},'_'};
        _ -> MatchSpec = {{Bucket,'$1'},'_'}
    end,
    MList = ets:match(State#state.t,MatchSpec),
    list(MList,[]).

is_empty(SrvRef) -> gen_server:call(SrvRef, is_empty).

drop(SrvRef) -> gen_server:call(SrvRef, drop).
    
fold(SrvRef, Fun, Acc0) -> gen_server:call(SrvRef, {fold, Fun, Acc0}, infinity).

fold_bucket_keys(SrvRef, Bucket, Fun, Acc0) ->
    gen_server:call(SrvRef, {fold_bucket_keys, Bucket, Fun, Acc0}, infinity).

%% Ignore callbacks for other backends so multi backend works
callback(_State, _Ref, _Msg) ->
    ok.

bev2_new_bucket_iterator(SrvRef, Idx) ->
    gen_server:call(SrvRef, {bev2_new_bucket_iterator, Idx}, infinity).

bev2_new_fold_iterator(SrvRef, Idx, Bucket, WantBKey, WantMd, WantVal) ->
    gen_server:call(SrvRef, {bev2_new_fold_iterator, Idx, Bucket, WantBKey, WantMd, WantVal}, infinity).

bev2_iterate(SrvRef, Iter) ->
    gen_server:call(SrvRef, {bev2_iterate, Iter}, infinity).

%% @private
%% TODO: put me back: handle_info(_Msg, State) -> {noreply, State}.
handle_info(_Msg, State) ->
    error_logger:error_msg("~s: handle_info: got ~p\n", [?MODULE, _Msg]),
    {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @private
do_bev2_iterate({bucket_iterator, Fun}, S) ->
    case Fun() of
        '$end_of_table' ->
            done;
        {Bucket, _K} = Key ->
            {continue, {undefined, undefined, Bucket},
             {bucket_iterator, fun() -> ets:next(S#state.t, Key) end}}
    end;
do_bev2_iterate({fold_iterator,
                 WantBKey, WantMd, WantVal, GenFun, StopTransFun}, S) ->
    case StopTransFun(GenFun()) of
        '$end_of_table' ->
            done;
        BK ->
            [{BK, Val}] = ets:lookup(S#state.t, BK),
            BKey = if WantBKey -> BK;
                      true     -> undefined
                   end,
            Value = if WantVal -> Val;
                       true    -> undefined
                    end,
            Res = {BKey, undefined, Value},
            NewGenFun = fun() -> ets:next(S#state.t, BK) end,
            NewI = {fold_iterator, WantBKey, WantMd, WantVal, NewGenFun,
                    StopTransFun},
            {continue, Res, NewI}
    end.

%% TODO: Add to docs: This iterator *may* return keys that are not
%%                    in the requested bucket.
%%    To avoid this feature, I believe that we need to return another
%%    value to the caller, e.g. {continue, NewCont}, to allow the
%%    caller to manage how much time it wants to spend folding.  This
%%    new continuation tells the caller that nothing matched this time.
%%    Otherwise, we could be folding for hours before we find a key that
%%    is in the desired bucket.

make_bev2_fold_iterator(_Idx, Bucket, WantBKey, WantMd, WantVal, S) ->
    GenFun = case Bucket of
                 undefined ->
                     fun() -> ets:first(S#state.t) end;
                 _ when S#state.ordered_p ->
                     FirstKey = {Bucket, <<>>},
                     case ets:member(S#state.t, FirstKey) of
                         true when S#state.ordered_p ->
                             fun() -> FirstKey end;
                         false when S#state.ordered_p ->
                             fun() -> ets:next(S#state.t, FirstKey) end
                     end;
                 _ ->
                     fun() -> ets:first(S#state.t) end
             end,
    StopTransFun = if S#state.ordered_p, Bucket /= undefined ->
                           fun({B, _K} = BKey) when B == Bucket ->
                                   BKey;
                              (_X) ->
                                   '$end_of_table'
                           end;
                      true ->
                           fun(X) -> X end
                   end,
    {fold_iterator, WantBKey, WantMd, WantVal, GenFun, StopTransFun}.

%%
%% Test
%%
-ifdef(TEST).

bogus_type_test() ->
    %% Weird, try riak_kv_backend:standard_test(?MODULE, [{ets_backend_table_type, bogus3}])
    %%        catch X:Y -> should_be_here end.
    %% ... doesn't catch.  What is EUnit doing?  What am I doing?  ....
    {Pid, Ref} = 
        spawn_monitor(fun() ->
            riak_kv_backend:standard_test(?MODULE, [{ets_backend_table_type, supposed_to_crash}]),
            exit(should_never_get_here)
        end),
    %% Double-weird, *both* receive clauses are necessary.  {sigh}  It's
    %% a random thing which of the two exceptions we get in the 'DOWN' msg.
    receive
        {'DOWN', Ref, process, Pid, _YY = {{badmatch, _}, _}} ->
            ok;
        {'DOWN', Ref, process, Pid, _YY = {badarg, _}} ->
            ok
    after 1000 ->
            exit(bogus_type_test_timeout)
    end.

simple_test() ->
    riak_kv_backend:standard_test(?MODULE, []).

simple_ordered_set_test() ->
    riak_kv_backend:standard_test(?MODULE, [{table_type, ordered_set}]).

-ifdef(EQC).
eqc_test() ->
    ?assertEqual(true, backend_eqc:test(?MODULE, true)).

-endif. % EQC
-endif. % TEST
