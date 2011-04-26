%% -------------------------------------------------------------------
%%
%% riak_kv_vnode_op_ctx: Request/transaction context for vnode ops
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

%% @doc Request/transaction context for vnode ops
-module(riak_kv_vnode_op_ctx).
-export([new/7,
         type/1,
         index/1,
         sender/1,
         bkey/1,
         request_object/1,
         existing_object/1,
         set_existing_object/2,
         proposed_object/1,
         set_proposed_object/2,
         merged_object/1,
         set_merged_object/2,
         final_object/1,
         set_final_object/2,
         decision/1,
         set_decision/2,
         abort_reason/1,
         set_abort_reason/2,
         options/1,
         get_option/2,
         get_option/3,
         returnbody/1,
         last_write_wins/1,
         req_id/1,
         prunetime/1,
         get_bucket_props/1,
         get_bucket_prop/2,
         get_bucket_prop/3,
         store_call/3,
         reply/2,
         reply/3]).

-record(riak_kv_vnode_op_ctx, {type, 
                               idx, 
                               storecall, 
                               sender, 
                               bkey, 
                               request_object, 
                               options,
                               existing_object, 
                               merged_object, 
                               proposed_object, 
                               final_object,
                               decision = abort :: commit | ignore | abort ,
                               abort_reason}).

new(Type, StoreCall, Idx, Sender, BKey, Object, Options) ->
    #riak_kv_vnode_op_ctx{type=Type,
                          idx=Idx, 
                          storecall=StoreCall,
                          sender=Sender, 
                          bkey=BKey, 
                          request_object=Object, 
                          options=Options}.

type(#riak_kv_vnode_op_ctx{type=Type}) -> Type.
index(#riak_kv_vnode_op_ctx{idx=Idx}) -> Idx.
sender(#riak_kv_vnode_op_ctx{sender=Sender}) -> Sender.
bkey(#riak_kv_vnode_op_ctx{bkey=BKey}) -> BKey.
request_object(#riak_kv_vnode_op_ctx{request_object=Object}) -> Object.
existing_object(#riak_kv_vnode_op_ctx{existing_object=Object}) -> Object.
set_existing_object(O,Object) -> O#riak_kv_vnode_op_ctx{existing_object=Object}.
proposed_object(#riak_kv_vnode_op_ctx{proposed_object=Object}) -> Object.
set_proposed_object(O,Object) -> O#riak_kv_vnode_op_ctx{proposed_object=Object}.
merged_object(#riak_kv_vnode_op_ctx{merged_object=Object}) -> Object.
set_merged_object(O, Object) -> O#riak_kv_vnode_op_ctx{merged_object=Object}.
final_object(#riak_kv_vnode_op_ctx{final_object=FinalObject}) -> FinalObject.
set_final_object(O, Object) -> O#riak_kv_vnode_op_ctx{final_object=Object}.
decision(#riak_kv_vnode_op_ctx{decision=Decision}) -> Decision.
set_decision(O, Decision) -> O#riak_kv_vnode_op_ctx{decision=Decision}.
abort_reason(#riak_kv_vnode_op_ctx{abort_reason=Reason}) -> Reason.
set_abort_reason(O, Reason) -> O#riak_kv_vnode_op_ctx{abort_reason=Reason}.
options(#riak_kv_vnode_op_ctx{options=Options}) -> Options.
get_option(K, O=#riak_kv_vnode_op_ctx{}) -> get_option(K, O, undefined).
get_option(K,O=#riak_kv_vnode_op_ctx{},D) -> proplists:get_value(K,options(O),D).
returnbody(O) -> get_option(returnbody, O, false).
last_write_wins(O) -> get_option(lww, O, false).
req_id(O) -> get_option(reqid, O, 0).
prunetime(O) -> get_option(prunetime, O, undefined).
get_bucket_props(O) -> get_option(bprops, O, []).
get_bucket_prop(K, O) -> get_bucket_prop(K, O, undefined).
get_bucket_prop(K, O, D) -> proplists:get_value(K, get_bucket_props(O), D).

store_call(#riak_kv_vnode_op_ctx{storecall=StoreCall}, Function, Args) ->
    StoreCall(Function, Args).

reply(O, Type) ->
    riak_core_vnode:reply(sender(O), {Type, index(O), req_id(O)}).

reply(O, Type, Arg) ->
    riak_core_vnode:reply(sender(O), {Type, index(O), Arg, req_id(O)}).    
    

    
    
    
