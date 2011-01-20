% -------------------------------------------------------------------
%%
%% riak_get_fsm: coordination of Riak GET requests
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

-module(riak_kv_get_fsm).
-behaviour(gen_fsm).
-include_lib("riak_kv_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start/6, pure_start/8]).
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).
-export([initialize/2,waiting_vnode_r/2,waiting_read_repair/2]).

-define(PURE_DRIVER, gen_fsm_test_driver).
-export([pure_unanimous/0, pure_conflict/0, pure_conflict_notfound/0]).

-record(state, {client :: {pid(), reference()},
                n :: pos_integer(), 
                r :: pos_integer(), 
                allowmult :: boolean(), 
                preflist :: [{pos_integer(), atom()}], 
                waiting_for :: [{pos_integer(), atom(), atom()}],
                req_id :: pos_integer(), 
                starttime :: pos_integer(), 
                replied_r :: list(), 
                replied_notfound :: list(),
                replied_fail :: list(),
                repair_sent :: list(), 
                final_obj :: undefined | {ok, riak_object:riak_object()} |
                             tombstone | {error, notfound},
                timeout :: pos_integer(),
                tref    :: reference(),
                bkey :: {riak_object:bucket(), riak_object:key()},
                ring :: riak_core_ring:riak_core_ring(),
                startnow :: {pos_integer(), pos_integer(), pos_integer()},
                pure_p :: boolean(),
                pure_opts :: list()
               }).

%% Inventory of impure actions:
%%  x riak_core_ring_manager:get_my_ring()
%%  x timer:send_after()
%%  x riak_core_util:chash_key()
%%  x riak_core_bucket:get_bucket()
%%  x Client ! ...
%%  x riak_core_node_watcher:nodes()
%%  x riak_kv_util:try_cast()
%%  x riak_core_util:moment()
%%  x spawn()
%%  x riak_kv_vnode:del()
%%  x riak_kv_vnode:readrepair()
%%  x riak_kv_stat:update()
%%
%% Impure functions that we know that we use but don't
%% care about:
%%  * now()

start(ReqId,Bucket,Key,R,Timeout,From) ->
    gen_fsm:start(?MODULE, {ReqId,Bucket,Key,R,Timeout,From,[]}, []).

pure_start(FsmID,ReqId,Bucket,Key,R,Timeout,From,PureOpts) ->
    ?PURE_DRIVER:start(FsmID, ?MODULE, {ReqId,Bucket,Key,R,Timeout,From,PureOpts}).

%% @private
init({ReqId,Bucket,Key,R,Timeout,Client,PureOpts}) ->
    PureP = proplists:get_value(debug, PureOpts, false),
    StateData0 = #state{client=Client,r=R, timeout=Timeout,
                req_id=ReqId, bkey={Bucket,Key},
                pure_p=PureP,pure_opts=PureOpts},
    {ok, Ring} = impure_get_my_ring(StateData0),
    StateData1 = StateData0#state{ring=Ring},
    {ok,initialize,StateData1,0}.



%% @private
initialize(timeout, StateData0=#state{timeout=Timeout, r=R0, req_id=ReqId,
                                      bkey={Bucket,Key}=BKey, ring=Ring,
                                      client=Client}) ->
    StartNow = now(),
    TRef = impure_timer_send_after(StateData0, Timeout),
    DocIdx = impure_riak_core_util_chash_key(StateData0, {Bucket, Key}),
    Req = #riak_kv_get_req_v1{
      bkey = BKey,
      req_id = ReqId
     },
    BucketProps = impure_get_bucket(StateData0, Bucket, Ring),
    N = proplists:get_value(n_val,BucketProps),
    R = riak_kv_util:expand_rw_value(r, R0, BucketProps, N),
    case R > N of
        true ->
            impure_bang(StateData0,
                        Client,
                        {ReqId, {error, {n_val_violation, N}}}),
            {stop, normal, StateData0};
        false -> 
            AllowMult = proplists:get_value(allow_mult,BucketProps),
            Preflist = riak_core_ring:preflist(DocIdx, Ring),
            {Targets, Fallbacks} = lists:split(N, Preflist),
            UpNodes = impure_riak_core_node_watcher_nodes(StateData0),
            {Sent1, Pangs1} = impure_riak_kv_util_try_cast(
                                StateData0, Req, UpNodes, Targets),
            Sent = 
                % Sent is [{Index,TargetNode,SentNode}]
                case length(Sent1) =:= N of   
                    true -> Sent1;
                    false -> Sent1 ++ riak_kv_util:fallback(Req, UpNodes, Pangs1,
                                                            Fallbacks)
                end,
            Moment = impure_riak_core_util_moment(StateData0),
            StateData = StateData0#state{n=N,r=R,
                                         allowmult=AllowMult,repair_sent=[],
                                         preflist=Preflist,final_obj=undefined,
                                         replied_r=[],replied_fail=[],
                                         replied_notfound=[],
                                         starttime=Moment,
                                         waiting_for=Sent,tref=TRef,
                                         startnow=StartNow},
            {next_state,waiting_vnode_r,StateData}
    end.

waiting_vnode_r({r, {ok, RObj}, Idx, ReqId},
                  StateData=#state{r=R,allowmult=AllowMult,
                                   req_id=ReqId,client=Client,
                                   replied_r=Replied0}) ->
    Replied = [{RObj,Idx}|Replied0],
    case length(Replied) >= R of
        true ->
            Final = respond(Client,Replied,AllowMult,ReqId,StateData),
            update_stats(StateData),
            NewStateData = StateData#state{replied_r=Replied,final_obj=Final},
            finalize(NewStateData);
        false ->
            NewStateData = StateData#state{replied_r=Replied},
            {next_state,waiting_vnode_r,NewStateData}
    end;
waiting_vnode_r({r, {error, notfound}, Idx, ReqId},
                  StateData=#state{r=R,allowmult=AllowMult,replied_fail=Fails,
                                   req_id=ReqId,client=Client,n=N,
                                   replied_r=Replied,
                                   replied_notfound=NotFound0}) ->
    NotFound = [Idx|NotFound0],
    NewStateData = StateData#state{replied_notfound=NotFound},
    FailThreshold = erlang:min(trunc((N/2.0)+1), % basic quorum, or
                               (N-R+1)), % cannot ever get R 'ok' replies
    %% FailThreshold = N-R+1,
    case (length(NotFound) + length(Fails)) >= FailThreshold of
        false ->
            {next_state,waiting_vnode_r,NewStateData};
        true ->
            update_stats(StateData),
            impure_bang(StateData,
                        Client,
                        {ReqId, {error,notfound}}),
            Final = merge(Replied,AllowMult),
            finalize(NewStateData#state{final_obj=Final})
    end;
waiting_vnode_r({r, {error, Err}, Idx, ReqId},
                  StateData=#state{r=R,client=Client,n=N,allowmult=AllowMult,
                                   replied_r=Replied,
                                   replied_fail=Failed0,req_id=ReqId,
                                   replied_notfound=NotFound}) ->
    Failed = [{Err,Idx}|Failed0],
    NewStateData = StateData#state{replied_fail=Failed},
    FailThreshold = erlang:min(trunc((N/2.0)+1), % basic quorum, or
                               (N-R+1)), % cannot ever get R 'ok' replies
    %% FailThreshold = N-R+1,
    case (length(Failed) + length(NotFound)) >= FailThreshold of
        false ->
            {next_state,waiting_vnode_r,NewStateData};
        true ->
            case length(NotFound) of
                0 ->
                    FullErr = [E || {E,_I} <- Failed],
                    update_stats(StateData),
                    impure_bang(StateData,
                                Client,
                                {ReqId, {error,FullErr}});
                _ ->
                    update_stats(StateData),
                    impure_bang(StateData,
                                Client,
                                {ReqId, {error,notfound}})
            end,
            Final = merge(Replied,AllowMult),
            finalize(NewStateData#state{final_obj=Final})
    end;
waiting_vnode_r(timeout, StateData=#state{client=Client,req_id=ReqId,replied_r=Replied,allowmult=AllowMult}) ->
    update_stats(StateData),
    impure_bang(StateData,
                Client,
                {ReqId, {error,timeout}}),
    really_finalize(StateData#state{final_obj=merge(Replied, AllowMult)}).

waiting_read_repair({r, {ok, RObj}, Idx, ReqId},
                  StateData=#state{req_id=ReqId,allowmult=AllowMult,replied_r=Replied0}) ->
    Replied = [{RObj,Idx}|Replied0],
    Final = merge(Replied,AllowMult),
    finalize(StateData#state{replied_r=Replied,final_obj=Final});
waiting_read_repair({r, {error, notfound}, Idx, ReqId},
                  StateData=#state{req_id=ReqId,replied_notfound=Replied0}) ->
    finalize(StateData#state{replied_notfound=[Idx|Replied0]});
waiting_read_repair({r, {error, Err}, Idx, ReqId},
                  StateData=#state{req_id=ReqId,replied_fail=Replied0}) ->
    finalize(StateData#state{replied_fail=[{Err,Idx}|Replied0]});
waiting_read_repair(timeout, StateData) ->
    really_finalize(StateData).

has_all_replies(#state{replied_r=R,replied_fail=F,replied_notfound=NF, n=N}) ->
    length(R) + length(F) + length(NF) >= N.

finalize(StateData=#state{replied_r=[]}) ->
    case has_all_replies(StateData) of
        true -> {stop,normal,StateData};
        false -> {next_state,waiting_read_repair,StateData}
    end;    
finalize(StateData) ->
    case has_all_replies(StateData) of
        true -> really_finalize(StateData);
        false -> {next_state,waiting_read_repair,StateData}
    end.

really_finalize(StateData=#state{final_obj=Final,
                                 waiting_for=Sent,
                                 replied_r=RepliedR,
                                 bkey=BKey,
                                 req_id=ReqId,
                                 replied_notfound=NotFound,
                                 starttime=StartTime}) ->
    case Final of
        tombstone ->
            maybe_finalize_delete(StateData);
        {ok,_} ->
            maybe_do_read_repair(Sent,Final,RepliedR,NotFound,BKey,
                                 ReqId,StartTime,StateData);
        _ -> nop
    end,
    {stop,normal,StateData}.

maybe_finalize_delete(StateData=#state{replied_notfound=NotFound,n=N,
                                       replied_r=RepliedR,
                                       waiting_for=Sent,req_id=ReqId,
                                       bkey=BKey}) ->
    F = fun() ->
    IdealNodes = [{I,Node} || {I,Node,Node} <- Sent],
    case length(IdealNodes) of
        N -> % this means we sent to a perfect preflist
            case (length(RepliedR) + length(NotFound)) of
                N -> % and we heard back from all nodes with non-failure
                    case lists:all(fun(X) -> riak_kv_util:is_x_deleted(X) end,
                                   [O || {O,_I} <- RepliedR]) of
                        true -> % and every response was X-Deleted, go!
                            [impure_riak_kv_vnode_del(
                               StateData, {Idx,Node}, BKey,ReqId) ||
                                {Idx,Node} <- IdealNodes];
                        _ -> nop
                    end;
                _ -> nop
            end;
        _ -> nop
    end
    end,
    if StateData#state.pure_p == false ->
            spawn(F);
       true ->
            F()
    end.

maybe_do_read_repair(Sent,Final,RepliedR,NotFound,BKey,ReqId,StartTime,StateData) ->
    Targets = ancestor_indices(Final, RepliedR) ++ NotFound,
    {ok, FinalRObj} = Final,
    case Targets of
        [] -> nop;
        _ ->
            [begin 
                 {Idx,_Node,Fallback} = lists:keyfind(Target, 1, Sent),
                 impure_riak_kv_vnode_readrepair(
                   StateData,
                   {Idx, Fallback}, BKey, FinalRObj, ReqId, 
                   StartTime, [{returnbody, false}])
             end || Target <- Targets],
            impure_riak_kv_stat_update(StateData, read_repairs)
    end.


%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info(timeout, StateName, StateData) ->
    ?MODULE:StateName(timeout, StateData);
%% @private
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

merge(VResponses, AllowMult) ->
   merge_robjs([R || {R,_I} <- VResponses],AllowMult).

respond(Client,VResponses,AllowMult,ReqId,StateData) ->
    Merged = merge(VResponses, AllowMult),
    case Merged of
        tombstone ->
            Reply = {error,notfound};
        {ok, Obj} ->
            case riak_kv_util:is_x_deleted(Obj) of
                true ->
                    Reply = {error, notfound};
                false ->
                    Reply = {ok, Obj}
            end;
        X ->
            Reply = X
    end,
    impure_bang(StateData,
                Client,
                {ReqId, Reply}),
    Merged.

merge_robjs([], _) ->
    {error, notfound};
merge_robjs(RObjs0,AllowMult) ->
    RObjs1 = [X || X <- [riak_kv_util:obj_not_deleted(O) ||
                            O <- RObjs0], X /= undefined],
    case RObjs1 of
        [] -> tombstone;
        _ ->
            RObj = riak_object:reconcile(RObjs0,AllowMult),
            {ok, RObj}
    end.

strict_descendant(O1, O2) ->
    vclock:descends(riak_object:vclock(O1),riak_object:vclock(O2)) andalso
    not vclock:descends(riak_object:vclock(O2),riak_object:vclock(O1)).

ancestor_indices({ok, Final},AnnoObjects) ->
    [Idx || {O,Idx} <- AnnoObjects, strict_descendant(Final, O)].


update_stats(#state{startnow=StartNow} = StateData) ->
    EndNow = now(),
    impure_riak_kv_stat_update(StateData,
                               {get_fsm_time, timer:now_diff(EndNow, StartNow)}).    

%% Impure handling stuff

impure_interp(Fun, Arg) when is_function(Fun, 1) ->
    Fun(Arg);
impure_interp(Else, _) ->
    Else.

impure_get_my_ring(#state{pure_p = false}) ->
    riak_core_ring_manager:get_my_ring();
impure_get_my_ring(#state{pure_opts = Pure_Opts}) ->
    {ok, proplists:get_value(get_my_ring, Pure_Opts)}.

impure_timer_send_after(#state{pure_p = false}, Timeout) ->
    erlang:send_after(Timeout, self(), timeout);
impure_timer_send_after(#state{pure_opts = Pure_Opts}, _Timeout) ->
    proplists:get_value(timer_send_after, Pure_Opts, dontcare).

impure_riak_core_util_chash_key(#state{pure_p = false}, BKey) ->
    riak_core_util:chash_key(BKey);
impure_riak_core_util_chash_key(#state{pure_opts = Pure_Opts}, BKey) ->
    Default = <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>,
    impure_interp(proplists:get_value(chash_key, Pure_Opts, Default),
                  BKey).

impure_get_bucket(#state{pure_p = false}, Bucket, Ring) ->
    riak_core_bucket:get_bucket(Bucket, Ring);
impure_get_bucket(#state{pure_opts = Pure_Opts}, Bucket, Ring) ->
    Default = [{name,Bucket},
               {n_val,2},
               {allow_mult,true},
               {last_write_wins,false},
               {big_vclock,1000},
               {young_vclock,-1},
               {precommit,[]},
               {postcommit,[]},
               {chash_keyfun,{riak_core_util,chash_std_keyfun}},
               {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
               {linkfun,{modfun,riak_kv_wm_link_walker,mapreduce_linkfun}},
               {old_vclock,86400},
               {young_vclock,20},
               {big_vclock,50},
               {small_vclock,10},
               {r,quorum},
               {w,quorum},
               {dw,quorum},
               {rw,quorum}],
    impure_interp(proplists:get_value(get_bucket, Pure_Opts, Default),
                  {Bucket, Ring}).

impure_bang(#state{pure_p = false}, Pid, Msg) ->
    Pid ! Msg;
impure_bang(#state{pure_opts = Pure_Opts}, Client, Msg) ->
    MyRef = proplists:get_value(my_ref, Pure_Opts),
    F = fun({Client1, Msg1}) ->
                gen_fsm_test_driver:add_trace(MyRef, {bang, Client1, Msg1})
        end,
    impure_interp(proplists:get_value(bang, Pure_Opts, F),
                  {Client, Msg}).

impure_riak_core_node_watcher_nodes(#state{pure_p = false}) ->
    riak_core_node_watcher:nodes(riak_kv);
impure_riak_core_node_watcher_nodes(#state{pure_opts = Pure_Opts}) ->
    proplists:get_value(node_watcher_nodes, Pure_Opts, crashme_watcher_nodelist).
    
impure_riak_kv_util_try_cast(#state{pure_p = false}, Req, UpNodes, Targets) ->
    riak_kv_util:try_cast(Req, UpNodes, Targets);
impure_riak_kv_util_try_cast(#state{pure_opts = Pure_Opts}, Req, UpNodes, Targets) ->
    MyRef = proplists:get_value(my_ref, Pure_Opts),
    CastTargets = proplists:get_value(cast_targets, Pure_Opts, [cast_targets_empty]),
    %% Return value must be {Sent1, Pangs1}
    F = fun({Req1, UpNodes1, Targets1}) ->
                gen_fsm_test_driver:add_trace(
                  MyRef, {try_cast, UpNodes1, Targets1}),
                [gen_fsm_test_driver:add_trace(
                  MyRef, {try_cast_to, Nd, Req1}) || Nd <- Targets1],
                {CastTargets, []}
        end,
    impure_interp(proplists:get_value(try_cast, Pure_Opts, F),
                  {Req, UpNodes, Targets}).

impure_riak_core_util_moment(#state{pure_p = false}) ->
    riak_core_util:moment();
impure_riak_core_util_moment(#state{pure_opts = Pure_Opts}) ->
    proplists:get_value(moment, Pure_Opts, fake_moment).

impure_riak_kv_vnode_del(#state{pure_p = false}, IdxNode, BKey, ReqId) ->
    riak_kv_vnode:del(IdxNode, BKey, ReqId);
impure_riak_kv_vnode_del(#state{pure_opts = Pure_Opts}, IdxNode, BKey, ReqId) ->
    MyRef = proplists:get_value(my_ref, Pure_Opts),
    F = fun({IdxNode1, BKey1, ReqId1}) ->
                gen_fsm_test_driver:add_trace(
                  MyRef, {vnode_del, IdxNode1, BKey1, ReqId1})
        end,
    impure_interp(proplists:get_value(vnode_del, Pure_Opts, F),
                  {IdxNode, BKey, ReqId}).

impure_riak_kv_vnode_readrepair(#state{pure_p = false}, IdxFallback, BKey,
                                FinalRObj, ReqId, StartTime, Pure_Opts) ->
    riak_kv_vnode:readrepair(IdxFallback, BKey,
                             FinalRObj, ReqId, StartTime, Pure_Opts);
impure_riak_kv_vnode_readrepair(#state{pure_opts = Pure_Opts}, IdxFallback, BKey,
                                FinalRObj, ReqId, StartTime, RRPure_Opts) ->
    MyRef = proplists:get_value(my_ref, Pure_Opts),
    F = fun({IdxFallback1, BKey1, FinalRObj1, ReqId1, StartTime1, RRPure_Opts1}) ->
                gen_fsm_test_driver:add_trace(
                  MyRef, {readrepair, IdxFallback1, BKey1, FinalRObj1, ReqId1, StartTime1, RRPure_Opts1})
        end,
    impure_interp(proplists:get_value(vnode_readrepair, Pure_Opts, F),
                  {IdxFallback, BKey, FinalRObj, ReqId, StartTime, RRPure_Opts}).

impure_riak_kv_stat_update(#state{pure_p = false}, Name) ->
    riak_kv_stat:update(Name);
impure_riak_kv_stat_update(#state{pure_opts = Pure_Opts}, Name) ->
    MyRef = proplists:get_value(my_ref, Pure_Opts),
    F = fun({Name1}) ->
                gen_fsm_test_driver:add_trace(
                  MyRef, {Name1})
        end,
    impure_interp(proplists:get_value(stat_update, Pure_Opts, F),
                  {Name}).

pure_unanimous() ->
    Ref = ref0,
    ReqID = req1,
    NumParts = 8, SeedNode = r1@node,
    Bucket = <<"b">>,
    Key = <<"k">>,
    Value = <<"42!">>,
    ClientID = <<"clientID1">>,
    Ring = chash:fresh(NumParts, SeedNode),
    Obj = riak_object:increment_vclock(
            riak_object:new(Bucket, Key, Value), ClientID),
    N = 2,
    Parts  = lists:sublist([Part || {Part, _} <- element(2, Ring)], N),
    CastTargets = [{Idx, SeedNode, SeedNode} || Idx <- Parts],

    PureOpts = [{debug,true},
                {my_ref, Ref},
                {get_my_ring, riak_core_ring:fresh(NumParts, SeedNode)},
                {timer_send_after, do_not_bother},
                %% Use pure get_bucket default (it's a long, tedious list)
                {node_watcher_nodes, [SeedNode]},
                %% The cast_targets prop is used by the default handler
                %% for impurt_riak_kv_util_try_cast, so don't delete it
                %% unless you're overriding the try_cast property.
                {cast_targets, CastTargets}
               ],
    InitIter = ?MODULE:pure_start(Ref, ReqID, Bucket, Key, quorum,
                                  5000, fake_client_pid, PureOpts),
    Events = [{send_event, {r, {ok, Obj}, Idx, ReqID}} ||
                 Idx <- Parts],
    X = ?PURE_DRIVER:run_to_completion(Ref, ?MODULE, InitIter, Events),
    [{res, X},
     {state, ?PURE_DRIVER:get_state(Ref)},
     {statedata, ?PURE_DRIVER:get_statedata(Ref)},
     {trace, ?PURE_DRIVER:get_trace(Ref)}].
    
pure_conflict() ->
    Ref = ref0,
    ReqID = req1,
    NumParts = 8, SeedNode = r1@node,
    Bucket = <<"b">>,
    Key = <<"k">>,
    Value = <<"42!">>,
    ClientID = <<"clientID1">>,
    Ring = chash:fresh(NumParts, SeedNode),
    Obj = riak_object:increment_vclock(
            riak_object:new(Bucket, Key, Value), ClientID),
    Obj2 = riak_object:increment_vclock(
             riak_object:new(Bucket, Key, <<"other copy sorry">>), <<"booID">>),
    N = 2,
    Parts  = lists:sublist([Part || {Part, _} <- element(2, Ring)], N),
    CastTargets = [{Idx, SeedNode, SeedNode} || Idx <- Parts],

    PureOpts = [{debug,true},
                {my_ref, Ref},
                {get_my_ring, riak_core_ring:fresh(NumParts, SeedNode)},
                {timer_send_after, do_not_bother},
                %% Use pure get_bucket default (it's a long, tedious list)
                {node_watcher_nodes, [SeedNode]},
                %% The cast_targets prop is used by the default handler
                %% for impurt_riak_kv_util_try_cast, so don't delete it
                %% unless you're overriding the try_cast property.
                {cast_targets, CastTargets}
               ],
    InitIter = ?MODULE:pure_start(Ref, ReqID, Bucket, Key, quorum,
                                  5000, fake_client_pid, PureOpts),
    Events = [{send_event, {r, {ok, Obj}, lists:nth(1, Parts), ReqID}},
              {send_event, {r, {ok, Obj2}, lists:nth(2, Parts), ReqID}}],
    X = ?PURE_DRIVER:run_to_completion(Ref, ?MODULE, InitIter, Events),
    [{res, X},
     {state, ?PURE_DRIVER:get_state(Ref)},
     {statedata, ?PURE_DRIVER:get_statedata(Ref)},
     {trace, ?PURE_DRIVER:get_trace(Ref)}].
    
pure_conflict_notfound() ->
    Ref = ref0,
    ReqID = req1,
    NumParts = 8, SeedNode = r1@node,
    Bucket = <<"b">>,
    Key = <<"k">>,
    Value = <<"42!">>,
    ClientID = <<"clientID1">>,
    Ring = chash:fresh(NumParts, SeedNode),
    Obj = riak_object:increment_vclock(
            riak_object:new(Bucket, Key, Value), ClientID),
    N = 3,
    Parts  = lists:sublist([Part || {Part, _} <- element(2, Ring)], N),
    CastTargets = [{Idx, SeedNode, SeedNode} || Idx <- Parts],

    PureOpts = [{debug,true},
                {my_ref, Ref},
                {get_my_ring, riak_core_ring:fresh(NumParts, SeedNode)},
                {timer_send_after, do_not_bother},
                %% Use pure get_bucket default (it's a long, tedious list)
                {node_watcher_nodes, [SeedNode]},
                %% The cast_targets prop is used by the default handler
                %% for impurt_riak_kv_util_try_cast, so don't delete it
                %% unless you're overriding the try_cast property.
                {cast_targets, CastTargets}
               ],
    InitIter = ?MODULE:pure_start(Ref, ReqID, Bucket, Key, quorum,
                                  5000, fake_client_pid, PureOpts),
    Events = [{send_event, {r, {ok, Obj}, lists:nth(1, Parts), ReqID}},
              {send_event, {r, {error, notfound}, lists:nth(2, Parts), ReqID}},
              {send_event, {r, {error, notfound}, lists:nth(3, Parts), ReqID}}],
    X = ?PURE_DRIVER:run_to_completion(Ref, ?MODULE, InitIter, Events),
    [{res, X},
     {state, ?PURE_DRIVER:get_state(Ref)},
     {statedata, ?PURE_DRIVER:get_statedata(Ref)},
     {trace, ?PURE_DRIVER:get_trace(Ref)}].
    
-ifdef(TEST).

fake_cover_test() ->
    %% These functions are purely for manual testing/usage demonstration,
    %% but we ought to execute them once for the coverage report's sake.
    _ = ?MODULE:pure_unanimous(),
    _ = ?MODULE:pure_conflict(),
    _ = ?MODULE:pure_conflict_notfound(),
    ok.

-endif. % TEST
