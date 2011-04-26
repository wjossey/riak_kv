%% -------------------------------------------------------------------
%%
%% riak_kv_vnode_op: State machine driver for vnode ops
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc State machine driver for vnode ops.
-module(riak_kv_vnode_op).
-include_lib("riak_kv_vnode.hrl").
-export([new/7, 
         execute/1]).
-record(rkv_vop, {mod, modstate, ctx}).

new(Mod, StoreCall, Index, Sender, BKey, Object, Options) ->
    Ctx = ?CTX:new(Mod, StoreCall, Index, Sender, BKey, Object, Options),
    {ok, NewCtx, ModState} =  Mod:initialize(Ctx),
    {ok, #rkv_vop{mod=Mod, modstate=ModState, ctx=NewCtx}}.

execute(O) -> decision(validate, O).

decision(validate, O) ->
    decision_test(op_call(validate, O), true, maybe_fetch_existing, abort);
decision(maybe_fetch_existing, O=#rkv_vop{ctx=Ctx}) ->
    ?CTX:reply(Ctx, w),
    decision_test(op_call(needs_existing_object,O),true,fetch_existing,abort);
decision(fetch_existing, O) ->
    decision_test_fn(O, fun fetch_existing/1, propose, abort);
decision(propose, O) -> 
    decision_test(op_call(propose, O), true, maybe_merge, abort);
decision(maybe_merge, O) ->
    decision_test(op_call(needs_syntactic_merge,O),true,prepare_merge,finalize);
decision(prepare_merge, O) ->
    case ctx_get(existing_object, O) of
        undefined ->
            decision_test_fn(O, fun fetch_existing/1, prepare_merge, abort);
        {error, notfound} ->
            decision(finalize, ctx_set(set_merged_object, 
                                       ctx_get(proposed_object, O), O));
        ExistObj ->
            NewO = do_syntactic_merge(ExistObj, ctx_get(proposed_object, O), 
                                      ctx_get(req_id, O), 
                                      ctx_get(prunetime, O), O),
            decision_test({ctx_get(decision, NewO), NewO}, 
                          commit, do_merge, maybe_abort)
    end;

decision(maybe_abort, O) ->
    decision_test({ctx_get(decision, O), O}, ignore, ignore, abort);
decision(ignore, O) ->
    decision_test(op_call(ignored, O), true, reply, reply);
decision(do_merge, O) ->
    decision_test(op_call(merge, O), true, finalize, abort);
decision(finalize, O) ->
    {FinalObj, NewO0} = do_finalize(O),
    PutArgs = [ctx_get(bkey, O), term_to_binary(FinalObj)],
    case store_call(put, PutArgs, NewO0) of
        {ok, NewO1} ->
            {_, NewO2} = op_call(committed, NewO1),
            decision(reply, ctx_set(set_decision, commit,NewO2));
        _ ->
            decision(abort, O)
    end;
decision(abort, O) ->
    decision_test(op_call(aborted, O), true, reply, reply);
decision(reply, O) ->
    case ctx_get(decision, O) of
        D when D =:= ignore orelse D =:= commit ->
            case ctx_get(returnbody, O) of
                true ->
                    do_reply(dw, ctx_get(final_object, O), O);
                false ->
                    do_reply(dw, O)
            end;
        abort ->
            do_reply(fail, O)
    end.

%%% operation helpers

set_existing({{ok, ExistingBin}, O}) ->
    {true, ctx_set(set_existing_object, binary_to_term(ExistingBin), O)};
set_existing({{error, notfound}, O}) ->
    {true, ctx_set(set_existing_object, {error, notfound}, O)};
set_existing({Other, O}) ->
    {false, ctx_set(set_existing_object, Other, O)}.
fetch_existing(O=#rkv_vop{ctx=Ctx}) ->
    set_existing(store_call(get, [?CTX:bkey(Ctx)], O)).


do_finalize(O) ->
    Obj = ctx_get(merged_object, O),
    PruneTime = ctx_get(prunetime, O),
    VC = riak_object:vclock(Obj),
    BProps = ctx_get(get_bucket_props, O),
    AMObj = riak_kv_vnode:enforce_allow_mult(Obj, BProps),
    case PruneTime of 
        undefined ->
            {AMObj, ctx_set(set_final_object, AMObj, O)};
        _ ->
            StoreObj = riak_object:set_vclock(
                         AMObj,vclock:prune(VC, PruneTime, BProps)),
            {StoreObj, ctx_set(set_final_object, StoreObj, O)}
    end.

do_reply(Type, #rkv_vop{ctx=Ctx}) ->
    ?CTX:reply(Ctx, Type).
    
do_reply(Type, Arg, #rkv_vop{ctx=Ctx}) ->
    ?CTX:reply(Ctx, Type, Arg).

do_syntactic_merge(Existing, Proposed, ReqId, PruneTime, O) ->
    MergedObj = riak_object:syntactic_merge(Existing, Proposed,
                                            term_to_binary(ReqId),
                                            PruneTime),
    case riak_object:vclock(MergedObj) =:= riak_object:vclock(Existing) of
        true ->
            ctx_set(set_merged_object,MergedObj,ctx_set(set_decision,ignore,O));
        false ->
            ctx_set(set_merged_object,MergedObj,ctx_set(set_decision,commit,O))
    end.

%%% call helpers

store_call(Fun, Args, O=#rkv_vop{ctx=Ctx}) ->
    {?CTX:store_call(Ctx, Fun, Args), O}.
    
op_call(Fun, O=#rkv_vop{mod=Mod, modstate=ModState, ctx=Ctx}) ->
    {Result, NewCtx, NewModState} =  Mod:Fun(Ctx, ModState),
    {Result, O#rkv_vop{modstate=NewModState, ctx=NewCtx}}.

ctx_set(Fun, Arg, O=#rkv_vop{ctx=Ctx}) ->
    O#rkv_vop{ctx=apply(?CTX, Fun, [Ctx,Arg])}.

ctx_get(Fun, #rkv_vop{ctx=Ctx}) ->
    apply(?CTX, Fun, [Ctx]).


%%% state-flow drivers

decision_test(Test,TestVal,TrueFlow,FalseFlow) ->
    case Test of
        {{propose, O}, NewO} ->
            decision_flow(TrueFlow, Test, ctx_set(set_proposed_object, O, NewO));
        {{merge, O}, NewO} ->
            decision_flow(TrueFlow, Test, ctx_set(set_merged_object, O, NewO));
        {{abort, NewO}} ->
            decision(abort, NewO);
        {TestVal,NewO} -> decision_flow(TrueFlow, Test, NewO);
        {_Other,NewO} -> decision_flow(FalseFlow, Test, NewO)
    end.

decision_test_fn({{error, Reason}, NewO}, _TestFn, _TrueFlow, _FalseFlow) ->
    decision(abort, ctx_set(set_decision, abort, ctx_set(set_abort_reason, Reason, NewO)));
decision_test_fn({{abort, Reason}, NewO}, _TestFn, _TrueFlow, _FalseFlow) ->
    decision(abort, ctx_set(set_decision, abort, ctx_set(set_abort_reason, Reason, NewO)));
decision_test_fn({{propose, O}, NewO}, TestFn, TrueFlow, _FalseFlow) ->
    decision_flow(TrueFlow, TestFn, ctx_set(set_proposed_object, O, NewO));
decision_test_fn({{merge, O}, NewO}, TestFn, TrueFlow, _FalseFlow) ->
    decision_flow(TrueFlow, TestFn, ctx_set(set_merged_object, O, NewO));
decision_test_fn(Test,TestFn,TrueFlow,FalseFlow) ->
    case TestFn(Test) of
        {true, NewO} -> decision_flow(TrueFlow, Test, NewO);
        {false, NewO} -> decision_flow(FalseFlow, Test, NewO)
    end.

decision_flow(X, _TestResult, O) -> decision(X, O).


