%% -------------------------------------------------------------------
%%
%% riak_util: functions that are useful throughout Riak
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


%% @doc Various functions that are useful throughout riak_kv.
-module(riak_kv_util).

-define(PURE_DRIVER, gen_fsm_test_driver).

-export([is_x_deleted/1,
         obj_not_deleted/1,
         try_cast/3,
         fallback/4,
         expand_rw_value/4,
         normalize_rw_value/2,
         make_request/2,
         fresh_test_ring/3,
         subst_test_ring_owners/2,
         make_fsm_pure_opts/5, make_fsm_pure_opts/6,
         make_test_bucket_props/1]).

-include_lib("riak_kv_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ===================================================================
%% Public API
%% ===================================================================

%% @spec is_x_deleted(riak_object:riak_object()) -> boolean()
%% @doc 'true' if all contents of the input object are marked
%%      as deleted; 'false' otherwise
%% @equiv obj_not_deleted(Obj) == undefined
is_x_deleted(Obj) ->
    case obj_not_deleted(Obj) of
        undefined -> true;
        _ -> false
    end.

%% @spec obj_not_deleted(riak_object:riak_object()) ->
%%          undefined|riak_object:riak_object()
%% @doc Determine whether all contents of an object are marked as
%%      deleted.  Return is the atom 'undefined' if all contents
%%      are marked deleted, or the input Obj if any of them are not.
obj_not_deleted(Obj) ->
    case [{M, V} || {M, V} <- riak_object:get_contents(Obj),
                    dict:is_key(<<"X-Riak-Deleted">>, M) =:= false] of
        [] -> undefined;
        _ -> Obj
    end.

%% @spec try_cast(term(), [node()], [{Index :: term(), Node :: node()}]) ->
%%          {[{Index :: term(), Node :: node(), Node :: node()}],
%%           [{Index :: term(), Node :: node()}]}
%% @doc Cast {Cmd, {Index,Node}, Msg} at riak_kv_vnode_master on Node
%%      if Node is in UpNodes.  The list of successful casts is the
%%      first element of the return tuple, and the list of unavailable
%%      nodes is the second element.  Used in riak_kv_put_fsm and riak_kv_get_fsm.
try_cast(Msg, UpNodes, Targets) ->
    try_cast(Msg, UpNodes, Targets, [], []).
try_cast(_Msg, _UpNodes, [], Sent, Pangs) -> {Sent, Pangs};
try_cast(Msg, UpNodes, [{Index,Node}|Targets], Sent, Pangs) ->
    case lists:member(Node, UpNodes) of
        false ->
            try_cast(Msg, UpNodes, Targets, Sent, [{Index,Node}|Pangs]);
        true ->
            gen_server:cast({riak_kv_vnode_master, Node}, make_request(Msg, Index)),
            try_cast(Msg, UpNodes, Targets, [{Index,Node,Node}|Sent],Pangs)
    end.

%% @spec fallback(term(), term(), [{Index :: term(), Node :: node()}],
%%                [{any(), Fallback :: node()}]) ->
%%         [{Index :: term(), Node :: node(), Fallback :: node()}]
%% @doc Cast {Cmd, {Index,Node}, Msg} at a node in the Fallbacks list
%%      for each node in the Pangs list.  Pangs should have come
%%      from the second element of the response tuple of a call to
%%      try_cast/3.
%%      Used in riak_kv_put_fsm and riak_kv_get_fsm

fallback(Cmd, UpNodes, Pangs, Fallbacks) ->
    fallback(Cmd, UpNodes, Pangs, Fallbacks, []).
fallback(_Cmd, _UpNodes, [], _Fallbacks, Sent) -> Sent;
fallback(_Cmd, _UpNodes, _Pangs, [], Sent) -> Sent;
fallback(Cmd, UpNodes, [{Index,Node}|Pangs], [{_,FN}|Fallbacks], Sent) ->
    case lists:member(FN, UpNodes) of
        false -> fallback(Cmd, UpNodes, [{Index,Node}|Pangs], Fallbacks, Sent);
        true ->
            gen_server:cast({riak_kv_vnode_master, FN}, make_request(Cmd, Index)),
            fallback(Cmd, UpNodes, Pangs, Fallbacks, [{Index,Node,FN}|Sent])
    end.


-spec make_request(vnode_req(), partition()) -> #riak_vnode_req_v1{}.
make_request(Request, Index) ->
    riak_core_vnode_master:make_request(Request,
                                        {fsm, undefined, self()},
                                        Index).

get_default_rw_val(Type, BucketProps) ->
    {ok, DefaultProps} = application:get_env(riak_core, default_bucket_props),
    case {proplists:get_value(Type, BucketProps),
          proplists:get_value(Type, DefaultProps)} of
        {undefined, Val} -> Val;
        {Val, undefined} -> Val;
        {Val1, _Val2} -> Val1
    end.
            
expand_rw_value(Type, default, BucketProps, N) ->
    normalize_rw_value(get_default_rw_val(Type, BucketProps), N);    
expand_rw_value(_Type, Val, _BucketProps, N) ->
    normalize_rw_value(Val, N).

normalize_rw_value(RW, _N) when is_integer(RW) -> RW;
normalize_rw_value(RW, N) when is_binary(RW) ->
    normalize_rw_value(binary_to_atom(RW, utf8), N);
normalize_rw_value(one, _N) -> 1;
normalize_rw_value(quorum, N) -> erlang:trunc((N/2)+1);
normalize_rw_value(all, N) -> N.

fresh_test_ring(NumParts, _PartitionInterval = PI, SeedNode) ->
    RingL = [{Idx, SeedNode} ||
                Idx <- lists:seq(0, (NumParts*PI) - 1, PI)],
    {NumParts, RingL}.

subst_test_ring_owners({NumParts, Ring0}, NewOwnerList) ->
    Ring = lists:map(
             fun({Part, _Node} = PN) ->
                     case proplists:get_value(Part, NewOwnerList) of
                         undefined ->
                             PN;
                         NewNode ->
                             {Part, NewNode}
                     end
             end, Ring0),
    {NumParts, Ring}.

make_fsm_pure_opts(FsmID, NumParts, SeedNode, PartitionInterval, Bucket) ->
    make_fsm_pure_opts(FsmID, NumParts, SeedNode, PartitionInterval,
                       Bucket, []).

make_fsm_pure_opts(FsmID, NumParts, SeedNode, PartitionInterval, Bucket, Opts)
  when PartitionInterval > 0 ->
    Ring0 = riak_kv_util:fresh_test_ring(NumParts, PartitionInterval, SeedNode),
    Ring = case proplists:get_value(partition_owners, Opts) of
               undefined ->
                   Ring0;
               Others ->
                   riak_kv_util:subst_test_ring_owners(Ring0, Others)
           end,
    ChState = {chstate, SeedNode, [], Ring, dict:new()}, % ugly hack
    [{debug, true},
     {my_ref, FsmID},
     {{erlang, node}, SeedNode},
     {{riak_core_ring_manager, get_my_ring}, {ok, ChState}},
     {{riak_core_node_watcher, nodes}, [SeedNode]},
     {{riak_core_bucket, get_bucket}, make_test_bucket_props(Bucket)},
     {timer_send_after, do_not_bother},
     {{riak_core_util, chash_key},
      fun(_) ->
              %% This magic hash will always use a preflist
              %% that is exactly equal to the ring's list
              %% of partitions.
              <<255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255,255>>
      end},
     {{riak_kv_util, try_cast},
      fun([Req1, UpNodes1, Targets1]) ->
              gen_fsm_test_driver:add_trace(
                FsmID, {try_cast, UpNodes1, Targets1}),
              [gen_fsm_test_driver:add_trace(
                 FsmID, {try_cast_to, Nd, Req1}) || Nd <- Targets1],
              CastTargets = [{Idx, Nd, Nd} || {Idx, Nd} <- Targets1],
              {CastTargets, []}
      end},
     {{riak_core_util, moment}, a_fake_moment},
     {{riak_kv_vnode, del},
      fun([Idx, BKey, ReqId]) ->
              gen_fsm_test_driver:add_trace(
                FsmID, {vnode_del, Idx, BKey, ReqId})
      end},
     {{riak_kv_vnode, readrepair},
      fun([IdxFallback1, BKey1, FinalRObj1, ReqId1, StartTime1, RRPureOpts1]) ->
              gen_fsm_test_driver:add_trace(
                FsmID, {readrepair, IdxFallback1, BKey1, FinalRObj1, ReqId1, StartTime1, RRPureOpts1})
      end},
     {{riak_kv_stat, update},
      fun([Name1]) ->
              gen_fsm_test_driver:add_trace(FsmID, Name1)
      end},
     {{erlang, '!'},
      fun([To, Msg]) ->
              ?PURE_DRIVER:add_trace(FsmID, {'!', To, Msg})
      end},
     {{app_helper, get_env},
      fun([riak_core, vnode_inactivity_timeout, _Default]) ->
              9999999999
      end},
     {{error_logger, error_msg},
      fun([Fmt, Args]) ->
              Str = io_lib:format(Fmt, Args),
              ?PURE_DRIVER:add_trace(FsmID, {error_logger, error_msg,
                                             lists:flatten(Str)})
      end},
     {{error_logger, info_msg},
      fun([Fmt, Args]) ->
              Str = io_lib:format(Fmt, Args),
              ?PURE_DRIVER:add_trace(FsmID, {error_logger, info_msg,
                                             lists:flatten(Str)})
      end}].

make_test_bucket_props(Bucket) ->
    [{name,Bucket},
     {n_val,3},
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
     {rw,quorum}].

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

normalize_test() ->
    3 = normalize_rw_value(3, 3),
    1 = normalize_rw_value(one, 3),
    2 = normalize_rw_value(quorum, 3),
    3 = normalize_rw_value(all, 3).

deleted_test() ->
    O = riak_object:new(<<"test">>, <<"k">>, "v"),
    false = is_x_deleted(O),
    MD = dict:new(),
    O1 = riak_object:apply_updates(
           riak_object:update_metadata(
             O, dict:store(<<"X-Riak-Deleted">>, true, MD))),
    true = is_x_deleted(O1).

-endif.
