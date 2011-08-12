-record(cont, {
          limit :: non_neg_integer(),
          prev_key :: riak_object:key(),
          'end' :: riak_object:key()
         }).
-type cont() :: #cont{} | done.
