%% -------------------------------------------------------------------
%%
%% riak_kv_yessir_backend: simulation backend for Riak
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc riak_kv_yessir_backend is a backend for benchmarking Riak without
%%      any disk I/O or RAM constraints.
%%
%% Riak: "Store this key/value pair."
%% Backend: "Yes, sir!"
%% Riak: "Get me that key/value pair."
%% Backend: "Yes, sir!"
%%
%% This backend uses zero disk resources and uses constant memory.
%%
%% * All put requests are immediately acknowledged 'ok'.  No
%%   data about the put request is stored.
%% * All get requests are fulfilled by creating a constant binary for
%%   the value.  No attempt is made to correlate get keys with
%%   previously-put keys or to correlate get values with previously-put
%%   values.
%%   - Get operation keys that are formatted in with the convention
%%     <<"integer.anything">> will use integer as the returned binary's
%%     Size.
%%
%% This backend is the Riak storage manager equivalent of:
%%
%% * cat > /dev/null
%% * cat < /dev/zero
%%
%% TODO list:
%%
%% * Add configuration option for random percent of not_found replies for get
%%   - Anything non-zero would trigger read-repair, which could be useful
%%     for some simulations.
%% * Is there a need for simulations for get to return different vclocks?
%% * Add variable latency before responding.  This callback API is
%%   synchronous, but adding constant- & uniform- & pareto-distributed
%%   delays would simulate disk I/O latencies because all other backend
%%   APIs are also synchronous.

-module(riak_kv_yessir_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([
         start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         list_bucket/2,
         fold/3,
         fold_keys/3,
         drop/1,
         is_empty/1,
         callback/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold]).

-record(state, {
          default_get = <<>>,
          op_get = 0,
          op_put = 0,
          op_delete = 0
         }).
-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start this backend, yes, sir!
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(_Partition, Config) ->
    DefaultLen = case proplists:get_value(default_size, Config) of
            undefined ->
                case application:get_env(riak_kv, yessir_backend_default_size) of
                    {ok, N} ->
                        N;
                    _ ->
                        spawn(fun() -> riak:stop("yessir_backend default_size unset, failing") end), timer:sleep(5000)
                end;
            Value ->
                Value
        end,
    {ok, #state{default_get = <<42:(DefaultLen*8)>>}}.

%% @doc Stop this backend, yes, sir!
-spec stop(state()) -> ok.
stop(_State) ->
    ok.

%% @doc Get a fake object, yes, sir!
get(S, {Bucket, Key}) ->
    Bin = case get_binsize(Key) of
              undefined    -> S#state.default_get;
              N            -> <<42:(N*8)>>
          end,
    O = riak_object:increment_vclock(riak_object:new(Bucket, Key, Bin),
                                     <<"yessir!">>, 1),
    {ok, term_to_binary(O)}.

%% @doc Store an object, yes, sir!
put(_S, {_Bucket, _Key}, _Val) ->
    ok.

%% @doc Delete an object, yes, sir!
delete(_S, {_Bucket, _Key}) ->
    ok.

list(_) ->
    [].

list_bucket(_, _) ->
    [].

fold(_, _, Acc) ->
    Acc.

fold_keys(_, _, Acc) ->
    Acc.

drop(_) ->
    ok.

is_empty(_) ->
    false.

callback(_, _, _) ->
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

get_binsize(<<X:8, _/binary>> = Bin) when $0 =< X, X =< $9->
    get_binsize(Bin, 0);
get_binsize(_) ->
    undefined.

get_binsize(<<X:8, Rest/binary>>, Val) when $0 =< X, X =< $9->
    get_binsize(Rest, (Val * 10) + (X - $0));
get_binsize(_, Val) ->
    Val.

%%
%% Test
%%
-ifdef(USE_BROKEN_TESTS).
-ifdef(TEST).
simple_test() ->
   Config = [],
   riak_kv_backend:standard_test(?MODULE, Config).

-ifdef(EQC).
eqc_test() ->
    Cleanup = fun(_State,_Olds) -> ok end,
    Config = [],
    ?assertEqual(true, backend_eqc:test(?MODULE, false, Config, Cleanup)).
-endif. % EQC
-endif. % TEST
-endif. % USE_BROKEN_TESTS
