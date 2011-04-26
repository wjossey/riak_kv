%% -------------------------------------------------------------------
%%
%% riak_kv_update_fsm: coordination of Riak UPDATE requests
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

%% @doc coordination of Riak UPDATE requests

-module(riak_kv_update_fsm).
%-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%-endif.
-include_lib("riak_kv_vnode.hrl").
-include_lib("riak_kv_js_pools.hrl").
-include("riak_kv_wm_raw.hrl").

-behaviour(gen_fsm).
-define(DEFAULT_OPTS, [{returnbody, false}, {update_last_modified, true}]).
-export([start_link/3]).
-ifdef(TEST).
-export([test_link/4]).
-endif.
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([prepare/2, validate/2, execute/2, waiting_vnode/2, finish/2]).


-type detail_info() :: timing.
-type detail() :: true |
                  false |
                  [detail_info()].

-type option() :: {pw, non_neg_integer()} | %% Min number of primary (owner) vnodes participating
                  {w,  non_neg_integer()} | %% Minimum number of vnodes receiving write
                  {dw, non_neg_integer()} | %% Minimum number of vnodes completing write
                  {timeout, timeout()} |
                  {details, detail()}.      %% Request additional details about request
                                            %% added as extra element at the end of result tuplezd 
-type options() :: [option()].

-export_type([option/0, options/0, detail/0, detail_info/0]).

-record(state, {from :: {raw, integer(), pid()},
                mfa :: {module(), atom(), term()},
                options=[] :: options(),
                n :: pos_integer(),
                w :: non_neg_integer(),
                dw :: non_neg_integer(),
                preflist2 :: riak_core_apl:preflist2(),
                bkey :: {riak_object:bucket(), riak_object:key()},
                req_id :: pos_integer(),
                starttime :: pos_integer(), % start time to send to vnodes
                replied_w :: list(),
                replied_dw :: list(),
                replied_fail :: list(),
                timeout :: pos_integer()|infinity,
                tref    :: reference(),
                vnode_options=[] :: list(),
                returnbody :: boolean(),
                resobjs=[] :: list(),
                allowmult :: boolean(),
                bucket_props:: list(),
                num_w = 0 :: non_neg_integer(),
                num_dw = 0 :: non_neg_integer(),
                num_fail = 0 :: non_neg_integer(),
                w_fail_threshold :: undefined | non_neg_integer(),
                dw_fail_threshold :: undefined | non_neg_integer(),
                final_obj :: undefined | riak_object:riak_object(),
                put_usecs :: undefined | non_neg_integer(),
                timing = [] :: [{atom(), {non_neg_integer(), non_neg_integer(),
                                          non_neg_integer()}}],
                reply % reply sent to client
               }).


-define(DEFAULT_TIMEOUT, 60000).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(From, UpdateReq, PutOptions) ->
    gen_fsm:start_link(?MODULE, [From, UpdateReq, PutOptions], []).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).
%% Create a put FSM for testing.  StateProps must include
%% starttime - start time in gregorian seconds
%% n - N-value for request (is grabbed from bucket props in prepare)
%% bkey - Bucket / Key
%% bucket_props - bucket properties
%% preflist2 - [{{Idx,Node},primary|fallback}] preference list
%% 
%% As test, but linked to the caller
test_link(From, UpdateReq, PutOptions, StateProps) ->
    gen_fsm:start_link(?MODULE, {test, [From, UpdateReq, PutOptions], StateProps}, []).

-endif.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([From, {BKey, MFA}, Options]) ->
    StateData = add_timing(prepare, #state{from = From,
                                           bkey = BKey,
                                           mfa = MFA,
                                           options = Options}),
    {ok, prepare, StateData, 0};
init({test, Args, StateProps}) ->
    %% Call normal init
    {ok, prepare, StateData, 0} = init(Args),

    %% Then tweak the state record with entries provided by StateProps
    Fields = record_info(fields, state),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    F = fun({Field, Value}, State0) ->
                Pos = proplists:get_value(Field, FieldPos),
                setelement(Pos, State0, Value)
        end,
    TestStateData = lists:foldl(F, StateData, StateProps),

    %% Enter into the validate state, skipping any code that relies on the
    %% state of the rest of the system
    {ok, validate, TestStateData, 0}.

%% @private
prepare(timeout, StateData0 = #state{bkey={Bucket,_Key}=BKey}) ->
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    DocIdx = riak_core_util:chash_key(BKey),
    N = proplists:get_value(n_val,BucketProps),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    Preflist2 = riak_core_apl:get_apl_ann(DocIdx, N, Ring, UpNodes),
    StartTime = riak_core_util:moment(),
    StateData = StateData0#state{n = N,
                                 bkey = BKey,
                                 bucket_props = BucketProps,
                                 preflist2 = Preflist2,
                                 starttime = StartTime},
    new_state_timeout(validate, StateData).
    
%% @private
validate(timeout, StateData0 = #state{from = {raw, ReqId, _Pid},
                                      options = Options0,
                                      n=N, bucket_props = BucketProps,
                                      preflist2 = Preflist2}) ->
    Timeout = get_option(timeout, Options0, ?DEFAULT_TIMEOUT),
    PW0 = get_option(pw, Options0, default),
    W0 = get_option(w, Options0, default),
    DW0 = get_option(dw, Options0, default),

    PW = riak_kv_util:expand_rw_value(pw, PW0, BucketProps, N),
    W = riak_kv_util:expand_rw_value(w, W0, BucketProps, N),

    %% Expand the DW value, but also ensure that DW <= W
    DW1 = riak_kv_util:expand_rw_value(dw, DW0, BucketProps, N),
    %% If no error occurred expanding DW also ensure that DW <= W
    case DW1 of
         error ->
             DW = error;
         _ ->
             DW = erlang:min(DW1, W)
    end,
    NumPrimaries = length([x || {_,primary} <- Preflist2]),
    NumVnodes = length(Preflist2),
    MinVnodes = erlang:max(1, erlang:max(W, DW)), % always need at least one vnode
    if
        PW =:= error ->
            process_reply({error, {pw_val_violation, PW0}}, StateData0);
        W =:= error ->
            process_reply({error, {w_val_violation, W0}}, StateData0);
        DW =:= error ->
            process_reply({error, {dw_val_violation, DW0}}, StateData0);
        (W > N) or (DW > N) or (PW > N) ->
            process_reply({error, {n_val_violation, N}}, StateData0);
        PW > NumPrimaries ->
            process_reply({error, {pw_val_unsatisfied, PW, NumPrimaries}}, StateData0);
        NumVnodes < MinVnodes ->
            process_reply({error, {insufficient_vnodes, NumVnodes,
                                   need, MinVnodes}}, StateData0);
        true ->
            AllowMult = proplists:get_value(allow_mult,BucketProps),
            StateData1 = StateData0#state{n=N, w=W, dw=DW, allowmult=AllowMult,
                                          req_id = ReqId,
                                          timeout = Timeout},
            Options = flatten_options(proplists:unfold(Options0 ++ ?DEFAULT_OPTS), []),
            StateData2 = handle_options(Options, StateData1),
            StateData = find_fail_threshold(StateData2),
            new_state_timeout(execute, StateData)
    end.


%% @private
execute(timeout, StateData0=#state{mfa=MFA, req_id = ReqId,
                                   timeout=Timeout, preflist2 = Preflist2, bkey=BKey,
                                   vnode_options=VnodeOptions0,
                                   options=Options,
                                   starttime = StartTime}) ->
    TRef = schedule_timeout(Timeout),
    Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2],
    VnodeOptions = lists:append([{update_time, now()}|Options], VnodeOptions0),
    riak_kv_vnode:update(Preflist, BKey, MFA, ReqId, StartTime, VnodeOptions),
    StateData = StateData0#state{
                  replied_w=[], replied_dw=[], replied_fail=[],
                  tref=TRef},
    case enough_results(StateData) of
        {reply, Reply, StateData1} ->
            process_reply(Reply, StateData1);
        {false, StateData} ->
            new_state(waiting_vnode, StateData)
    end.

%% @private
waiting_vnode(request_timeout, StateData) ->
    process_reply({error,timeout}, StateData);
waiting_vnode(Result, StateData) ->
    StateData1 = add_vnode_result(Result, StateData),
    case enough_results(StateData1) of
        {reply, Reply, StateData2} ->
            process_reply(Reply, StateData2);
        {false, StateData2} ->
            {next_state, waiting_vnode, StateData2}
    end.


finish(timeout, StateData = #state{timing = Timing, reply = Reply}) ->
    case Reply of
        {error, _} ->
            ok;
        _Ok ->
            %% TODO: Improve reporting of timing
            %% For now can add debug tracers to view the return from calc_timing
            {Duration, _Stages} = calc_timing(Timing),
            riak_kv_stat:update({put_fsm_time, Duration})
    end,
    {stop, normal, StateData};
finish(Reply, StateData) -> % late responses - add to state
    StateData1 = add_vnode_result(Reply, StateData),
    {next_state, finish, StateData1, 0}.

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private

handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% Move to the new state, marking the time it started
new_state(StateName, StateData) ->
    {next_state, StateName, add_timing(StateName, StateData)}.

%% Move to the new state, marking the time it started and trigger an immediate
%% timeout.
new_state_timeout(StateName, StateData) ->
    {next_state, StateName, add_timing(StateName, StateData), 0}.

%% What to do once enough responses from vnodes have been received to reply
process_reply(Reply, StateData) ->
    StateData1 = client_reply(Reply, StateData),
    new_state_timeout(finish, StateData1).

%%
%% Given an expanded proplist of options, take the first entry for any given key
%% and ignore the rest
%%
%% @private
flatten_options([], Opts) ->
    Opts;
flatten_options([{Key, Value} | Rest], Opts) ->
    case lists:keymember(Key, 1, Opts) of
        true ->
            flatten_options(Rest, Opts);
        false ->
            flatten_options(Rest, [{Key, Value} | Opts])
    end.

%% @private
handle_options([], State) ->
    State;
handle_options([{update_last_modified, false}|T], State) ->
    handle_options(T, State);
handle_options([{update_last_modified, true}|T], State = #state{}) ->
    %%handle_options(T, State#state{robj = update_last_modified(RObj)})
    handle_options(T, State);
handle_options([{returnbody, true}|T], State) ->
    VnodeOpts = [{returnbody, true} | State#state.vnode_options],
    %% Force DW>0 if requesting return body to ensure the dw event 
    %% returned by the vnode includes the object.
    handle_options(T, State#state{vnode_options=VnodeOpts,
                                  dw=erlang:max(1,State#state.dw),
                                  returnbody=true});
handle_options([{returnbody, false}|T], State) ->
    handle_options(T, State#state{returnbody=false});
handle_options([{_,_}|T], State) -> handle_options(T, State).

find_fail_threshold(State = #state{n = N, w = W, dw = DW}) ->
    State#state{ w_fail_threshold = N-W+1,    % cannot ever get W replies
                 dw_fail_threshold = N-DW+1}. % cannot ever get DW replies

%% Add a vnode result to the state structure and update the counts
add_vnode_result({w, Idx, _ReqId}, StateData = #state{replied_w = Replied,
                                                      num_w = NumW}) ->
    StateData#state{replied_w = [Idx | Replied], num_w = NumW + 1};
add_vnode_result({dw, Idx, _ReqId}, StateData = #state{replied_dw = Replied,
                                                       num_dw = NumDW}) ->
    StateData#state{replied_dw = [Idx | Replied], num_dw = NumDW + 1};
add_vnode_result({dw, Idx, ResObj, _ReqId}, StateData = #state{replied_dw = Replied,
                                                               resobjs = ResObjs,
                                                               num_dw = NumDW}) ->
    StateData#state{replied_dw = [Idx | Replied],
                    resobjs = [ResObj | ResObjs],
                    num_dw = NumDW + 1};
add_vnode_result({fail, Idx, _ReqId}, StateData = #state{replied_fail = Replied,
                                                         num_fail = NumFail}) ->
    StateData#state{replied_fail = [Idx | Replied],
                    num_fail = NumFail + 1};
add_vnode_result(_Other, StateData = #state{num_fail = NumFail}) ->
    %% Treat unrecognized messages as failures
    StateData#state{num_fail = NumFail + 1}.

enough_results(StateData = #state{w = W, num_w = NumW, dw = DW, num_dw = NumDW,
                                  num_fail = NumFail,
                                  w_fail_threshold = WFailThreshold,
                                  dw_fail_threshold = DWFailThreshold}) ->
    if
        NumW >= W andalso NumDW >= DW ->
            maybe_return_body(StateData);
        
        NumW >= W andalso NumFail >= DWFailThreshold ->
            {reply, {error,too_many_fails}, StateData};
        
        NumW < W andalso NumFail >= WFailThreshold ->
            {reply, {error,too_many_fails}, StateData};
        
        true ->
            {false, StateData}
    end.

maybe_return_body(StateData = #state{returnbody=false}) ->
    {reply, ok, StateData};
maybe_return_body(StateData = #state{resobjs = ResObjs, allowmult = AllowMult,
                                     returnbody = ReturnBody}) ->
    ReplyObj = merge_robjs(ResObjs, AllowMult),
    Reply = case ReturnBody of
                true  -> {ok, ReplyObj};
                false -> ok
            end,
    {reply, Reply, StateData#state{final_obj = ReplyObj}}.

make_vtag(RObj) ->
    <<HashAsNum:128/integer>> = crypto:md5(term_to_binary(riak_object:vclock(RObj))),
    riak_core_util:integer_to_list(HashAsNum,62).


merge_robjs(RObjs0,AllowMult) ->
    RObjs1 = [X || X <- RObjs0,
                   X /= undefined],
    case RObjs1 of
        [] -> {error, notfound};
        _ -> riak_object:reconcile(RObjs1,AllowMult)
    end.

get_option(Name, Options, Default) ->
    proplists:get_value(Name, Options, Default).

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) ->
    erlang:send_after(Timeout, self(), request_timeout).

client_reply(Reply, State = #state{from = {raw, ReqId, Pid}, options = Options}) ->
    State2 = add_timing(reply, State),
    Reply2 = case proplists:get_value(details, Options, false) of
                 false ->
                     Reply;
                 [] ->
                     Reply;
                 Details ->
                     add_client_info(Reply, Details, State2)
             end,
    Pid ! {ReqId, Reply2},
    add_timing(reply, State2#state{reply = Reply}).

add_client_info(Reply, Details, State) ->
    Info = client_info(Details, State, []),
    case Reply of
        ok ->
            {ok, Info};
        {OkError, ObjReason} ->
            {OkError, ObjReason, Info}
    end.

client_info(true, StateData, Info) ->
    client_info(default_details(), StateData, Info);
client_info([], _StateData, Info) ->
    Info;
client_info([timing | Rest], StateData = #state{timing = Timing}, Info) ->
    %% Duration is time from receiving request to responding
    {ResponseUsecs, Stages} = calc_timing(Timing),
    client_info(Rest, StateData, [{response_usecs, ResponseUsecs},
                                  {stages, Stages} | Info]).

default_details() ->
    [timing].


%% Add timing information to the state
add_timing(Stage, State = #state{timing = Timing}) ->
    State#state{timing = [{Stage, os:timestamp()} | Timing]}.

%% Calc timing information - stored as {Stage, StageStart} in reverse order. 
%% ResponseUsecs is calculated as time from reply to start.
calc_timing([{Stage, Now} | Timing]) ->
    ReplyNow = case Stage of
                   reply ->
                       Now;
                   _ ->
                       undefined
               end,
    calc_timing(Timing, Now, ReplyNow, []).

%% Each timing stage has start time.
calc_timing([], StageEnd, ReplyNow, Stages) ->
    %% StageEnd is prepare time
    {timer:now_diff(ReplyNow, StageEnd), Stages}; 
calc_timing([{reply, ReplyNow}|_]=Timing, StageEnd, undefined, Stages) ->
    %% Populate ReplyNow then handle normally.
    calc_timing(Timing, StageEnd, ReplyNow, Stages);
calc_timing([{Stage, StageStart} | Rest], StageEnd, ReplyNow, Stages) ->
    calc_timing(Rest, StageStart, ReplyNow,
                [{Stage, timer:now_diff(StageEnd, StageStart)} | Stages]).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

make_vtag_test() ->
    Obj = riak_object:new(<<"b">>,<<"k">>,<<"v1">>),
    ?assertNot(make_vtag(Obj) =:=
               make_vtag(riak_object:increment_vclock(Obj,<<"client_id">>))).

-endif.
