%% -------------------------------------------------------------------
%%
%% riak_kv_vnode_op_update: UPDATE vnode operation
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

%% @doc VNode operation for the UPDATE command
-module(riak_kv_vnode_op_update).
-include_lib("riak_kv_vnode.hrl").
-export([initialize/1,
         validate/2,
         needs_existing_object/2,
         propose/2,
         needs_syntactic_merge/2,
         merge/2,
         finalize/2,
         ignored/2,
         aborted/2,
         committed/2]).
-record(state, {op_fun}).

initialize(Ctx) ->
    {M, F, A} = ?CTX:request_object(Ctx),
    OpFun = fun(O, C) -> apply(M,F,lists:append([[?CTX:bkey(C),O],A,[C]])) end,
    {ok, Ctx, #state{op_fun=OpFun}}.

validate(Ctx, State) -> {true, Ctx, State}.

needs_existing_object(Ctx, State) -> {true, Ctx, State}.

propose(Ctx, State=#state{op_fun=OpFun}) -> 
    {ok, Obj0} = OpFun(?CTX:existing_object(Ctx), Ctx),
    VNodeOpts = ?CTX:get_option(vnode_options, Ctx),
    Obj = riak_object:apply_updates(
            riak_kv_put_fsm:update_last_modified(
              riak_object:increment_vclock(
                Obj0, proplists:get_value(client_id, VNodeOpts)))),
    {{propose, Obj}, Ctx, State}.

needs_syntactic_merge(Ctx, State) -> 
    {?CTX:last_write_wins(Ctx) =:= false, Ctx, State}.

merge(Ctx, State) -> {true, Ctx, State}.
finalize(Ctx, State) -> {true, Ctx, State}.
ignored(Ctx, State) -> {true, Ctx, State}.
aborted(Ctx, State) -> {true, Ctx, State}.
committed(Ctx, State) -> {true, Ctx, State}.

