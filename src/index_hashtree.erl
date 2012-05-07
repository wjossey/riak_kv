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

update(Id, Tree) ->
    gen_server:call(Tree, {update_tree, Id}).

build(Tree) ->
    gen_server:cast(Tree, build).

exchange_bucket(Id, Level, Bucket, Tree) ->
    gen_server:call(Tree, {exchange_bucket, Id, Level, Bucket}).

exchange_segment(Id, Segment, Tree) ->
    gen_server:call(Tree, {exchange_segment, Id, Segment}).

compare(Id, Remote, Tree) ->
    gen_server:call(Tree, {compare, Id, Remote}).

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
                                    case Idx of
                                        Partition ->
                                            insert(Idx, term_to_binary(BKey), hash_object(RObj), Tree);
                                        _ ->
                                            ok
                                    end,
                                    %% io:format("K: ~p~n", [Key]),
                                    %% io:format("I: ~p~n", [Idx]),
                                    ok
                            end,
                    acc0=ok},
    spawn_link(
      fun() ->
              riak_core_vnode_master:sync_command({Partition, node()},
                                                  Req,
                                                  riak_kv_vnode_master, infinity),
              ok
      end).

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
    %% Tree = orddict:fetch(Id, Trees),
    %% Tree2 = hashtree:update_tree(Tree),
    %% Trees2 = orddict:store(Id, Tree2, Trees),
    %% {reply, ok, State#state{trees=Trees2}};
    {_, State2} = apply_tree(Id,
                             fun(Tree) ->
                                     {ok, hashtree:update_tree(Tree)}
                             end,
                             State),
    {reply, ok, State2};
handle_call({exchange_bucket, Id, Level, Bucket}, _From, State) ->
    {Result, State2} =
        apply_tree(Id,
                   fun(Tree) ->
                           Result = hashtree:get_bucket(Level, Bucket, Tree),
                           {Result, Tree}
                   end,
                   State),
    {reply, Result, State2};

handle_call({exchange_segment, Id, Segment}, _From, State) ->
    {Result, State2} =
        apply_tree(Id,
                   fun(Tree) ->
                           [{_, Result}] = hashtree:key_hashes(Tree, Segment),
                           {Result, Tree}
                   end,
                   State),
    {reply, Result, State2};

handle_call({compare, Id, Remote}, _From, State) ->
    {Result, State2} =
        apply_tree(Id,
                   fun(Tree) ->
                           Result = hashtree:compare(Tree, Remote),
                           {Result, Tree}
                   end,
                   State),
    {reply, Result, State2};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(build, State=#state{index=Index}) ->
    fold_keys(Index, self()),
    {noreply, State};

handle_cast({insert, Id, Key, Hash}, State=#state{trees=Trees}) ->
    lager:info("Insert into ~p/~p :: ~p / ~p", [State#state.index, Id, Key, Hash]),
    Tree = orddict:fetch(Id, Trees),
    Tree2 = hashtree:insert(Key, Hash, Tree),
    Trees2 = orddict:store(Id, Tree2, Trees),
    {noreply, State#state{trees=Trees2}};
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
    Tree = orddict:fetch(Id, Trees),
    {Result, Tree2} = Fun(Tree),
    Trees2 = orddict:store(Id, Tree2, Trees),
    {Result, State#state{trees=Trees2}}.
