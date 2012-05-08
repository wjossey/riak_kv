-module(index_hashtree).
-behaviour(gen_server).

-include_lib("riak_kv_vnode.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {index,
                trees}).

-compile(export_all).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Index) ->
    gen_server:start_link(?MODULE, [Index], []).

new_tree(Id, Tree) ->
    gen_server:call(Tree, {new_tree, Id}, infinity).

insert(Id, Key, Hash, Tree) ->
    gen_server:cast(Tree, {insert, Id, Key, Hash}).

insert_object(BKey, RObj, Tree) ->
    gen_server:cast(Tree, {insert_object, BKey, RObj}).

update(Id, Tree) ->
    gen_server:call(Tree, {update_tree, Id}, infinity).

build(Tree) ->
    gen_server:cast(Tree, build).

exchange_bucket(Id, Level, Bucket, Tree) ->
    gen_server:call(Tree, {exchange_bucket, Id, Level, Bucket}, infinity).

exchange_segment(Id, Segment, Tree) ->
    gen_server:call(Tree, {exchange_segment, Id, Segment}, infinity).

compare(Id, Remote, Tree) ->
    gen_server:call(Tree, {compare, Id, Remote}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Index]) ->
    {ok, #state{index=Index, trees=orddict:new()}}.

hash_object(RObjBin) ->
    RObj = binary_to_term(RObjBin),
    Vclock = riak_object:vclock(RObj),
    UpdObj = riak_object:set_vclock(RObj, lists:sort(Vclock)),
    Hash = erlang:phash2(term_to_binary(UpdObj)),
    term_to_binary(Hash).

fold_keys(Partition, Tree) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Req = ?FOLD_REQ{foldfun=fun(BKey={Bucket,Key}, RObj, _) ->
                                    ChashKey = riak_core_util:chash_key({Bucket, Key}),
                                    Idx=riak_core_ring:responsible_index(ChashKey, Ring),
                                    insert(Idx, term_to_binary(BKey), hash_object(RObj), Tree),
                                    ok
                            end,
                    acc0=ok},
    %% spawn_link(
    %%   fun() ->
    %%           riak_core_vnode_master:sync_command({Partition, node()},
    %%                                               Req,
    %%                                               riak_kv_vnode_master, infinity),
    %%           ok
    %%   end).
    %%
    %% Need to block for now to ensure building happens before updating/compare.
    %% Easy to fix in the future to allow async building.
    riak_core_vnode_master:sync_command({Partition, node()},
                                        Req,
                                        riak_kv_vnode_master, infinity),
    ok.

handle_call({new_tree, Id}, _From, State=#state{trees=Trees}) ->
    NewTree = case Trees of
                  [] ->
                      hashtree:new(Id);
                  [{_,Other}|_] ->
                      hashtree:new(Id, Other)
              end,
    Trees2 = orddict:store(Id, NewTree, Trees),
    State2 = State#state{trees=Trees2},
    {reply, ok, State2};
handle_call({update_tree, Id}, _From, State) ->
    lager:info("Updating tree: (vnode)=~p (responsible idx)=~p", [State#state.index, Id]),
    apply_tree(Id,
               fun(Tree) ->
                       {ok, hashtree:update_tree(Tree)}
               end,
               State);

handle_call({exchange_bucket, Id, Level, Bucket}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       Result = hashtree:get_bucket(Level, Bucket, Tree),
                       {Result, Tree}
               end,
               State);

handle_call({exchange_segment, Id, Segment}, _From, State) ->
    apply_tree(Id,
               fun(Tree) ->
                       [{_, Result}] = hashtree:key_hashes(Tree, Segment),
                       {Result, Tree}
               end,
               State);

handle_call({compare, Id, Remote}, From, State) ->
    Tree = orddict:fetch(Id, State#state.trees),
    spawn(fun() ->
                  Result = hashtree:compare(Tree, Remote),
                  gen_server:reply(From, Result)
          end),
    {noreply, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(build, State=#state{index=Index}) ->
    lager:info("Starting build: ~p", [Index]),
    fold_keys(Index, self()),
    lager:info("Finished build: ~p", [Index]),
    {noreply, State};

handle_cast({insert, Id, Key, Hash}, State) ->
    State2 = do_insert(Id, Key, Hash, State),
    {noreply, State2};
handle_cast({insert_object, BKey, RObj}, State) ->
    lager:info("Inserting object ~p", [BKey]),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ChashKey = riak_core_util:chash_key(BKey),
    Idx=riak_core_ring:responsible_index(ChashKey, Ring),
    State2 = do_insert(Idx, term_to_binary(BKey), hash_object(RObj), State),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _) ->
    ok.
%% terminate(_Reason, #state{trees=Trees}) ->
%%     case Trees of
%%         [] ->
%%             ok;
%%         [Tree|_] ->
%%             hashtree:destroy(Tree),
%%             ok
%%     end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

apply_tree(Id, Fun, State=#state{trees=Trees}) ->
    case orddict:find(Id, Trees) of
        error ->
            {reply, not_responsible, State};
        {ok, Tree} ->
            {Result, Tree2} = Fun(Tree),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State2 = State#state{trees=Trees2},
            {reply, Result, State2}
    end.

do_insert(Id, Key, Hash, State=#state{trees=Trees}) ->
    lager:info("Insert into ~p/~p :: ~p / ~p", [State#state.index, Id, Key, Hash]),
    Tree = orddict:fetch(Id, Trees),
    Tree2 = hashtree:insert(Key, Hash, Tree),
    Trees2 = orddict:store(Id, Tree2, Trees),
    State#state{trees=Trees2}.
