%% -------------------------------------------------------------------
%%
%% riak_kv_backend: Riak backend behaviour 
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

-module(riak_kv_backend).
-export([behaviour_info/1]).
-export([callback_after/3]).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([standard_test/2]).
-endif.

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{capability,0},       % () -> proplist()
     {capability,2},       % (State, Bucket::binary()) -> proplist()
     {start,2},            % (Partition, Config)
     {stop,1},             % (State) 
     {get,2},              % (State, BKey)
     {put,3},              % (State, BKey, Val)
     {list,1},             % (State)
     {list_bucket,2},      % (State, Bucket)
     {delete,2},           % (State, BKey)
     {drop,1},             % (State)
     {fold,3},             % (State, fun({B,K},V,Acc), Acc), 
     {fold_bucket_keys,4}, % (State, Bucket, fun(K, V, Acc), Acc)
     {is_empty,1},         % (State)
     {callback,3}];        % (State, Ref, Msg) ->
behaviour_info(_Other) ->
    undefined.

%% Queue a callback for the backend after Time ms.
-spec callback_after(integer(), reference(), term()) -> reference().
callback_after(Time, Ref, Msg) when is_integer(Time), is_reference(Ref) ->
    riak_core_vnode:send_command_after(Time, {backend_callback, Ref, Msg}).

-ifdef(TEST).

%% NOTE: standard_test/2 isn't run directly by EUnit when it tries to test
%%       this module.  However, it should be called by every backend that
%%       we support.
%%       foreach i ( src/*backend.erl )
%%           echo -n $i
%%           grep :standard_test $i | wc -l
%%       end
%%       src/riak_kv_backend.erl       0
%%       src/riak_kv_bitcask_backend.erl       2
%%       src/riak_kv_cache_backend.erl       1
%%       src/riak_kv_dets_backend.erl       1
%%       src/riak_kv_ets_backend.erl       1
%%       src/riak_kv_fs_backend.erl       1
%%       src/riak_kv_gb_trees_backend.erl       1
%%       src/riak_kv_multi_backend.erl       1

standard_test(BackendMod, Config) ->
    {ok, S} = BackendMod:start(42, Config),
    ?assertEqual(ok, BackendMod:put(S,{<<"b1">>,<<"k1">>},<<"v1">>)),
    ?assertEqual(ok, BackendMod:put(S,{<<"b2">>,<<"k2">>},<<"v2">>)),
    ?assertEqual({ok,<<"v2">>}, BackendMod:get(S,{<<"b2">>,<<"k2">>})),
    ?assertEqual({error, notfound}, BackendMod:get(S, {<<"b1">>,<<"k3">>})),
    ?assertEqual([{<<"b1">>,<<"k1">>},{<<"b2">>,<<"k2">>}],
                 lists:sort(BackendMod:list(S))),
    ?assertEqual([<<"k2">>], BackendMod:list_bucket(S, <<"b2">>)),
    ?assertEqual([<<"k1">>], BackendMod:list_bucket(S, <<"b1">>)),
    ?assertEqual([<<"k1">>], BackendMod:list_bucket(
                               S, {filter, <<"b1">>, fun(_K) -> true end})),
    ?assertEqual([], BackendMod:list_bucket(
                       S, {filter, <<"b1">>, fun(_K) -> false end})),
    BucketList = BackendMod:list_bucket(S, '_'),
    ?assert(lists:member(<<"b1">>, BucketList)),
    ?assert(lists:member(<<"b2">>, BucketList)),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b2">>,<<"k2">>})),
    ?assertEqual({error, notfound}, BackendMod:get(S, {<<"b2">>, <<"k2">>})),
    ?assertEqual([{<<"b1">>, <<"k1">>}], BackendMod:list(S)),
    Folder = fun(K, V, A) -> [{K,V}|A] end,
    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>}], BackendMod:fold(S, Folder, [])),
    ?assertEqual(ok, BackendMod:put(S,{<<"b3">>,<<"k3">>},<<"v3">>)),
    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>},
                  {{<<"b3">>,<<"k3">>},<<"v3">>}], lists:sort(BackendMod:fold(S, Folder, []))),
    ?assertEqual(false, BackendMod:is_empty(S)),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b1">>,<<"k1">>})),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b3">>,<<"k3">>})),
    ?assertEqual(true, BackendMod:is_empty(S)),
    ok = BackendMod:stop(S).

-endif. % TEST
