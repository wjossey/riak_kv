-module(index_hashtree).
-behaviour(gen_server).

-include_lib("riak_kv_vnode.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {index,
                built,
                lock :: undefined | reference(),
                path,
                exchange_queue,
                trees}).

-compile(export_all).

%% TODO: For testing/dev, make this small. For prod, we should raise this.
-define(TICK_TIME, 10000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Index) ->
    gen_server:start_link(?MODULE, [Index], []).

start_link(Index, IndexN) ->
    gen_server:start_link(?MODULE, [Index, IndexN], []).

new_tree(Id, Tree) ->
    gen_server:call(Tree, {new_tree, Id}, infinity).

insert(Id, Key, Hash, Tree) ->
    insert(Id, Key, Hash, Tree, []).

insert(Id, Key, Hash, Tree, Options) ->
    gen_server:cast(Tree, {insert, Id, Key, Hash, Options}).

insert_object(BKey, RObj, Tree) ->
    gen_server:cast(Tree, {insert_object, BKey, RObj}).

start_exchange_remote(FsmPid, IndexN, Tree) ->
    gen_server:call(Tree, {start_exchange_remote, FsmPid, IndexN}, infinity).

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

get_lock(Tree, Type) ->
    get_lock(Tree, Type, self()).

get_lock(Tree, Type, Pid) ->
    gen_server:call(Tree, {get_lock, Type, Pid}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Index]) ->
    %% schedule_tick(),
    Root = "data/anti",
    Path = filename:join(Root, integer_to_list(Index)),

    entropy_manager:register_tree(Index, self()),
    {ok, #state{index=Index,
                trees=orddict:new(),
                built=false,
                path=Path,
                exchange_queue=[]}};

init([Index, IndexN]) ->
    %% schedule_tick(),
    Root = "data/anti",
    Path = filename:join(Root, integer_to_list(Index)),

    entropy_manager:register_tree(Index, self()),
    State = #state{index=Index,
                   trees=orddict:new(),
                   built=false,
                   path=Path,
                   exchange_queue=[]},
    State2 = init_trees(IndexN, State),
    {ok, State2}.

init_trees(IndexN, State) ->
    State2 = lists:foldl(fun(Id, StateAcc) ->
                                 do_new_tree(Id, StateAcc)
                         end, State, IndexN),
    Built = load_built(State2),
    State2#state{built=Built}.

load_built(#state{trees=Trees}) ->
    {_,Tree0} = hd(Trees),
    case hashtree:read_meta(<<"built">>, Tree0) of
        {ok, <<1>>} ->
            true;
        _ ->
            false
    end.

hash_object(RObjBin) ->
    RObj = binary_to_term(RObjBin),
    Vclock = riak_object:vclock(RObj),
    UpdObj = riak_object:set_vclock(RObj, lists:sort(Vclock)),
    Hash = erlang:phash2(term_to_binary(UpdObj)),
    term_to_binary(Hash).

fold_keys(Partition, Tree) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Req = ?FOLD_REQ{foldfun=fun(BKey={Bucket,Key}, RObj, _) ->
                                    IndexN = get_index_n({Bucket, Key}, Ring),
                                    insert(IndexN, term_to_binary(BKey), hash_object(RObj),
                                           Tree, [if_missing]),
                                    ok
                            end,
                    acc0=ok},
    riak_core_vnode_master:sync_command({Partition, node()},
                                        Req,
                                        riak_kv_vnode_master, infinity),
    ok.

handle_call({new_tree, Id}, _From, State) ->
    State2 = do_new_tree(Id, State),
    {reply, ok, State2};

handle_call({get_lock, Type, Pid}, _From, State) ->
    {Reply, State2} = do_get_lock(Type, Pid, State),
    {reply, Reply, State2};

handle_call({start_exchange_remote, FsmPid, _IndexN}, _From, State) ->
    case entropy_manager:get_lock(exchange_remote, FsmPid) of
        max_concurrency ->
            {reply, max_concurrency, State};
        ok ->
            case do_get_lock(remote_fsm, FsmPid, State) of
                {ok, State2} ->
                    {reply, ok, State2};
                {Reply, State2} ->
                    {reply, Reply, State2}
            end
    end;

handle_call({update_tree, Id}, _From, State) ->
    lager:info("Updating tree: (vnode)=~p (preflist)=~p", [State#state.index, Id]),
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

%% handle_cast({exchange_status, Pid, RemoteVN, Reply},
%%             State=#state{exchanging={FsmPid,_}}) when Pid == FsmPid ->
handle_cast({exchange_status, _Pid, RemoteVN, Reply}, State) ->
    %% lager:info("S: ~p", [Reply]),
    case Reply of
        max_concurrency ->
            %% lager:info("Requeuing rate limited exchange"),
            State2 = requeue_exchange(RemoteVN, State);
        {remote, max_concurrency} ->
            %% lager:info("Requeuing rate limited exchange"),
            State2 = requeue_exchange(RemoteVN, State);
        {remote, already_exchanging} ->
            %% lager:info("Requeuing rate limited exchange"),
            State2 = requeue_exchange(RemoteVN, State);
        ok ->
            State2 = State;
        _ ->
            lager:info("Exchange status: ~p", [Reply]),
            State2 = State
    end,
    {noreply, State2};

handle_cast(tick, State) ->
    State2 = do_tick(State),
    %% schedule_tick(),
    {noreply, State2};

handle_cast(build, State) ->
    State2 = maybe_build(State),
    {noreply, State2};

handle_cast(build_failed, State) ->
    State2 = State#state{built=false},
    {noreply, State2};
handle_cast(build_finished, State) ->
    State2 = do_build_finished(State),
    {noreply, State2};

handle_cast({insert, Id, Key, Hash, Options}, State) ->
    State2 = do_insert(Id, Key, Hash, Options, State),
    {noreply, State2};
handle_cast({insert_object, BKey, RObj}, State) ->
    %% lager:info("Inserting object ~p", [BKey]),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexN = get_index_n(BKey, Ring),
    State2 = do_insert(IndexN, term_to_binary(BKey), hash_object(RObj), [], State),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = do_tick(State),
    {noreply, State2};

handle_info({'DOWN', Ref, _, _, _}, State) ->
    State2 = maybe_release_lock(Ref, State),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_new_tree(Id, State=#state{trees=Trees, path=Path}) ->
    IdBin = tree_id(Id),
    NewTree = case Trees of
                  [] ->
                      hashtree:new(IdBin, [{segment_path, Path}]);
                  [{_,Other}|_] ->
                      hashtree:new(IdBin, Other)
              end,
    Trees2 = orddict:store(Id, NewTree, Trees),
    State#state{trees=Trees2}.

do_get_lock(_, _, State) when State#state.built /= true ->
    lager:info("Not built: ~p :: ~p", [State#state.index, State#state.built]),
    {not_built, State};
do_get_lock(_Type, Pid, State=#state{lock=undefined}) ->
    Ref = monitor(process, Pid),
    State2 = State#state{lock=Ref},
    %% lager:info("Locked: ~p", [State#state.index]),
    {ok, State2};
do_get_lock(_, _, State) ->
    lager:info("Already locked: ~p", [State#state.index]),
    {already_locked, State}.

maybe_release_lock(Ref, State) ->
    case State#state.lock of
        Ref ->
            %% lager:info("Unlocked: ~p", [State#state.index]),
            State#state{lock=undefined};
        _ ->
            State
    end.

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

do_build_finished(State=#state{index=Index, built=_Pid}) ->
    lager:info("Finished build (b): ~p", [Index]),
    {_,Tree0} = hd(State#state.trees),
    hashtree:write_meta(<<"built">>, <<1>>, Tree0),
    State#state{built=true}.

do_insert(Id, Key, Hash, Opts, State=#state{trees=Trees}) ->
    %% lager:info("Insert into ~p/~p :: ~p / ~p", [State#state.index, Id, Key, Hash]),
    case orddict:find(Id, Trees) of
        {ok, Tree} ->
            Tree2 = hashtree:insert(Key, Hash, Tree, Opts),
            Trees2 = orddict:store(Id, Tree2, Trees),
            State#state{trees=Trees2};
        _ ->
            handle_unexpected_key(Id, Key, State),
            State
    end.

handle_unexpected_key(Id, Key, #state{index=Partition}) ->
    RP = riak_kv_vnode:responsible_preflists(Partition),
    case lists:member(Id, RP) of
        false ->
            lager:warning("Object ~p encountered during fold over partition "
                          "~p, but key does not hash to an index handled by "
                          "this partition", [Key, Partition]),
            ok;
        true ->
            lager:info("Partition/tree ~p/~p does not exist to hold object ~p",
                       [Partition, Id, Key]),
            ok
    end.

tree_id({Index, N}) ->
    %% hashtree is hardcoded for 22-byte (176-bit) tree id
    <<Index:160/integer,N:16/integer>>;
tree_id(_) ->
    erlang:error(badarg).

get_index_n(BKey) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    get_index_n(BKey, Ring).

get_index_n({Bucket, Key}, Ring) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket, Ring),
    N = proplists:get_value(n_val, BucketProps),
    ChashKey = riak_core_util:chash_key({Bucket, Key}),
    Index = riak_core_ring:responsible_index(ChashKey, Ring),
    {Index, N}.

do_all_exchange(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllExchanges = all_pairwise_exchanges(Index, Ring),
    LocalVN = {Index, node()},
    lists:map(
      fun(NextExchange) ->
              do_exchange(LocalVN, NextExchange, Ring),
              lager:info("===")
      end, AllExchanges),
    ok.

do_exchange(LocalVN, {RemoteIdx, IndexN}, Ring) ->
    Owner = riak_core_ring:index_owner(Ring, RemoteIdx),
    RemoteVN = {RemoteIdx, Owner},
    {ok, Fsm} = exchange_fsm:start_link(LocalVN, RemoteVN, IndexN),
    exchange_fsm:sync_start_exchange(Fsm),
    ok.

all_pairwise_exchanges(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    all_pairwise_exchanges(Index, Ring).

all_pairwise_exchanges(Index, Ring) ->
    LocalIndexN = riak_kv_vnode:responsible_preflists(Index, Ring),
    Sibs = riak_kv_vnode:preflist_siblings(Index),
    lists:flatmap(
      fun(RemoteIdx) when RemoteIdx == Index ->
              [];
         (RemoteIdx) ->
              RemoteIndexN = riak_kv_vnode:responsible_preflists(Index, Ring),
              SharedIndexN = ordsets:intersection(ordsets:from_list(LocalIndexN),
                                                  ordsets:from_list(RemoteIndexN)),
              [{RemoteIdx, IndexN} || IndexN <- SharedIndexN]
      end, Sibs).

start_exchange(_, _, _, State) when State#state.built /= true ->
    {not_built, State};
start_exchange(_, _, _, State) when State#state.lock /= undefined ->
    {already_exchanging, State};
start_exchange(LocalVN, {RemoteIdx, IndexN}, Ring, State) ->
    %% lager:info("Starting exchange: ~p", [LocalVN]),
    Owner = riak_core_ring:index_owner(Ring, RemoteIdx),
    RemoteVN = {RemoteIdx, Owner},
    case exchange_fsm:start(LocalVN, RemoteVN, IndexN) of
        {ok, FsmPid} ->
            %% Make this happen automatically as part of init in exchange_fsm
            %% lager:info("Starting exchange: ~p", [LocalVN]),
            exchange_fsm:start_exchange(FsmPid, self()),
            {ok, State};
        {error, Reason} ->
            {Reason, State}
    end.

schedule_tick() ->
    Tick = ?TICK_TIME,
    timer:apply_after(Tick, gen_server, cast, [self(), tick]).

do_tick(State) ->
    %% State1 = maybe_clear(State),
    State2 = maybe_build(State),
    State3 = maybe_exchange(State2),
    State3.

maybe_clear(State=#state{lock=undefined, built=true, trees=Trees}) ->
    lager:info("Clearing tree ~p", [State#state.index]),
    {_,Tree0} = hd(Trees),
    hashtree:destroy(Tree0),
    %% TODO: Consider re-generating reponsible preflists here to determine
    %%       IndexN. Could also solve bucket prop change issue.
    {IndexN,_} = lists:unzip(Trees),
    State2 = init_trees(IndexN, State#state{trees=orddict:new()}),
    State2#state{built=false};
maybe_clear(State) ->
    State.

maybe_build(State=#state{built=false, index=Index}) ->
    Self = self(),
    Pid = spawn_link(fun() ->
                             case entropy_manager:get_lock(build) of
                                 max_concurrency ->
                                     gen_server:cast(Self, build_failed);
                                 ok ->
                                     lager:info("Starting build: ~p", [Index]),
                                     fold_keys(Index, Self),
                                     lager:info("Finished build (a): ~p", [Index]),
                                     gen_server:cast(Self, build_finished)
                             end
                     end),
    State#state{built=Pid};
maybe_build(State) ->
    %% Already built or build in progress
    State.

maybe_exchange(State=#state{index=Index}) ->
    %% TODO: Shouldn't always increment next-exchange on all failure cases
    %%       ie. max_concurrency
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {NextExchange, State2} = next_exchange(Ring, State),
    LocalVN = {Index, node()},
    case start_exchange(LocalVN, NextExchange, Ring, State2) of
        {ok, State3} ->
            State3;
        {_Reason, State3} ->
            %% lager:info("ExchangeQ: ~p", [Reason]),
            State3
    end.

next_exchange(Ring, State=#state{exchange_queue=[]}) ->
    [Exchange|Rest] = all_pairwise_exchanges(State#state.index, Ring),
    State2 = State#state{exchange_queue=Rest},
    {Exchange, State2};
next_exchange(_Ring, State=#state{exchange_queue=Exchanges}) ->
    [Exchange|Rest] = Exchanges,
    State2 = State#state{exchange_queue=Rest},
    {Exchange, State2}.

requeue_exchange(Exchange, State) ->
    Exchanges = State#state.exchange_queue ++ [Exchange],
    State#state{exchange_queue=Exchanges}.
