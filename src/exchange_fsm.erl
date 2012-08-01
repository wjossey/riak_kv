-module(exchange_fsm).
-behaviour(gen_fsm).

%% API
-export([start_link/3, start/3]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).
-compile(export_all).

-record(state, {local,
                remote,
                index_n,
                remote_tree,
                lock,
                built,
                missing,
                from}).

%% Per state transition timeout used by certain transitions
-define(DEFAULT_ACTION_TIMEOUT, 20000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(LocalVN, RemoteVN, IndexN) ->
    gen_fsm:start_link(?MODULE, [LocalVN, RemoteVN, IndexN], []).

start(LocalVN, RemoteVN, IndexN) ->
    gen_fsm:start(?MODULE, [LocalVN, RemoteVN, IndexN], []).

sync_start_exchange(Fsm) ->
    gen_fsm:sync_send_event(Fsm, start_exchange, infinity).

start_exchange(Fsm) ->
    gen_fsm:send_event(Fsm, start_exchange).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([LocalVN, RemoteVN, IndexN]) ->
    State = #state{local=LocalVN,
                   remote=RemoteVN,
                   index_n=IndexN,
                   built=0},
    {ok, start_exchange, State}.

start_exchange(start_exchange, From, State) ->
    start_exchange(start_exchange, State#state{from=From}).

start_exchange(start_exchange, State=#state{local=LocalVN,
                                            remote=RemoteVN,
                                            index_n=IndexN}) ->
    {Index, _} = LocalVN,
    case index_hashtree:get_exchange_lock(Index) of
        {error, max_concurrency} ->
            lager:info("Exchange: max_concurrency"),
            {stop, normal, State};
        {ok, Lock} ->
            Sender = {fsm, undefined, self()},
            riak_core_vnode_master:command(RemoteVN,
                                           {start_exchange_remote, self(), IndexN},
                                           Sender,
                                           riak_kv_vnode_master),
            State2 = State#state{lock=Lock},
            next_state_with_timeout(start_exchange, State2)
    end;
start_exchange(timeout, State) ->
    do_timeout(State);
start_exchange({remote_exchange, Pid}, State) when is_pid(Pid) ->
    State2 = State#state{remote_tree=Pid},
    update_trees(start_exchange, State2);
start_exchange({remote_exchange, Error}, State) ->
    lager:info("Exchange: {remote, ~p}", [Error]),
    maybe_reply(Error, State),
    {stop, normal, State}.

update_trees(start_exchange, State=#state{local=LocalVN,
                                          remote=RemoteVN,
                                          index_n=IndexN}) ->
    lager:info("Sending to ~p", [LocalVN]),
    lager:info("Sending to ~p", [RemoteVN]),

    Sender = {fsm, undefined, self()},
    riak_core_vnode_master:command(LocalVN,
                                   {update_tree, IndexN},
                                   Sender,
                                   riak_kv_vnode_master),
    riak_core_vnode_master:command(RemoteVN,
                                   {update_tree, IndexN},
                                   Sender,
                                   riak_kv_vnode_master),
    next_state_with_timeout(update_trees, State);

update_trees(timeout, State) ->
    do_timeout(State);
update_trees({not_responsible, VNodeIdx, IndexN}, State) ->
    lager:info("VNode ~p does not cover preflist ~p", [VNodeIdx, IndexN]),
    maybe_reply({not_responsible, VNodeIdx, IndexN}, State),
    {stop, normal, State};
update_trees({tree_built, _, _}, State) ->
    %% lager:info("R: ~p", [Result]),
    Built = State#state.built + 1,
    %% {stop, normal, State}.
    case Built of
        2 ->
            lager:info("Moving to key exchange"),
            {next_state, key_exchange, State, 0};
        _ ->
            next_state_with_timeout(update_trees, State#state{built=Built})
    end.

key_exchange(timeout, State=#state{local=LocalVN,
                                   remote=RemoteVN,
                                   index_n=IndexN}) ->
    lager:info("Starting key exchange between ~p and ~p", [LocalVN, RemoteVN]),
    lager:info("Exchanging hashes for preflist ~p", [IndexN]),
    %% R1 = riak_core_vnode_master:sync_command(LocalVN,
    %%                                          {exchange_bucket, Index, 1, 0},
    %%                                          riak_kv_vnode_master),
    %% R2 = riak_core_vnode_master:sync_command(RemoteVN,
    %%                                          {exchange_bucket, Index, 1, 0},
    %%                                          riak_kv_vnode_master),


    %% lager:info("R1: ~p", [R1]),
    %% lager:info("R2: ~p", [R2]),

    Remote = fun(get_bucket, {L, B}) ->
                     exchange_bucket(RemoteVN, IndexN, L, B);
                (key_hashes, Segment) ->
                     exchange_segment(RemoteVN, IndexN, Segment)
             end,

    R = riak_core_vnode_master:sync_command(LocalVN,
                                            {compare_trees, IndexN, Remote},
                                            riak_kv_vnode_master),

    {keydiff,_,_,KeyDiff} = R,
    Missing = [binary_to_term(BKey) || {_, BKey} <- KeyDiff],
    [riak_kv_vnode:get([LocalVN, RemoteVN], BKey, make_ref()) || BKey <- Missing],
    {next_state, test, State#state{missing=Missing}, 1000}.

test(timeout, State=#state{missing=Missing}) ->
    {ok, RC} = riak:local_client(),
    [begin
         lager:info("Anti-entropy forced read repair: ~p/~p", [Bucket, Key]),
         RC:get(Bucket, Key)
     end || {Bucket, Key} <- Missing],
    maybe_reply(ok, State),
    {stop, normal, State};
test(Request, State) ->
    lager:info("Req: ~p", [Request]),
    {next_state, test, State, 500}.
    
exchange_bucket(VN, IndexN, Level, Bucket) ->
    riak_core_vnode_master:sync_command(VN,
                                        {exchange_bucket, IndexN, Level, Bucket},
                                        riak_kv_vnode_master).

exchange_segment(VN, IndexN, Segment) ->
    riak_core_vnode_master:sync_command(VN,
                                        {exchange_segment, IndexN, Segment},
                                        riak_kv_vnode_master).

handle_event(_Event, _StateName, State) ->
    %% {next_state, StateName, State}.
    {stop, badmsg, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    %% {reply, ok, StateName, State}.
    {stop, badmsg, State}.

handle_info(_Info, _StateName, State) ->
    %% {next_state, StateName, State}.
    {stop, badmsg, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_timeout(State=#state{local=LocalVN,
                        remote=RemoteVN,
                        index_n=IndexN}) ->
    lager:info("Timeout during exchange between (local) ~p and (remote) ~p, "
               "(preflist) ~p", [LocalVN, RemoteVN, IndexN]),
    maybe_reply({timeout, RemoteVN, IndexN}, State),
    {stop, normal, State}.

maybe_reply(_, #state{from=undefined}) ->
    ok;
maybe_reply(Reply, #state{from=From}) ->
    gen_fsm:reply(From, Reply).

next_state_with_timeout(StateName, State) ->
    next_state_with_timeout(StateName, State, ?DEFAULT_ACTION_TIMEOUT).
next_state_with_timeout(StateName, State, Timeout) ->
    {next_state, StateName, State, Timeout}.
