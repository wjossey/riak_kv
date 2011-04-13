-include_lib("riak_core/include/riak_core_vnode.hrl").

-type put_option() :: {returnbody, boolean()} |
                      {last_write_wins, boolean()} |
                      {rr, boolean()} |
                      rr | 
                      {bucket_props, riak_core_bucket:bucket_props()}.
-type put_options() :: [put_option()].


-record(riak_kv_put_req_v1, {
          bkey :: riak_object:bkey(),
          object :: riak_object:riak_object(),
          req_id :: riak_client:req_id(),
          start_time :: pos_integer(),
          options :: put_options()}).

-record(riak_kv_get_req_v1, {
          bkey :: riak_object:bkey(),
          req_id :: riak_client:req_id()}).

-record(riak_kv_mget_req_v1, {
          bkeys :: [riak_object:bkey()],
          req_id :: riak_client:req_id(),
          from :: term()}).

-record(riak_kv_listkeys_req_v1, {
          bucket :: riak_object:bucket(),
          req_id :: riak_client:req_id()}).

-record(riak_kv_listkeys_req_v2, {
          bucket :: riak_object:bucket()|'_'|tuple(),
          req_id :: riak_client:req_id(),
          caller :: pid()}).

-record(riak_kv_delete_req_v1, {
          bkey :: riak_object:bkey(),
          req_id :: riak_client:req_id()}).

-record(riak_kv_map_req_v1, {
          bkey :: riak_object:bkey(),
          qterm :: term(),
          keydata :: term(),
          from :: term()}).

-record(riak_kv_vclock_req_v1, {
          bkeys = [] :: [riak_object:bkey()]
         }).

-define(KV_PUT_REQ, #riak_kv_put_req_v1).
-define(KV_GET_REQ, #riak_kv_get_req_v1).
-define(KV_MGET_REQ, #riak_kv_mget_req_v1).
-define(KV_LISTKEYS_REQ, #riak_kv_listkeys_req_v2).
-define(KV_DELETE_REQ, #riak_kv_delete_req_v1).
-define(KV_MAP_REQ, #riak_kv_map_req_v1).
-define(KV_VCLOCK_REQ, #riak_kv_vclock_req_v1).

-type riak_kv_any_req() :: ?KV_PUT_REQ{} | ?KV_GET_REQ{} | ?KV_MGET_REQ{} |
                           ?KV_LISTKEYS_REQ{} | ?KV_DELETE_REQ{} | ?KV_MAP_REQ{} |
                           ?KV_VCLOCK_REQ{} | ?FOLD_REQ{}.

-type riak_kv_vnode_req() :: vnode_req(riak_kv_any_req()).
