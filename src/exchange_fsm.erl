-module(exchange_fsm).
-behaviour(gen_fsm).

%% API
-export([start_link/3]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).
-compile(export_all).

-record(state, {local,
                remote,
                index,
                built}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(LocalVN, RemoteVN, Index) ->
    gen_fsm:start_link(?MODULE, [LocalVN, RemoteVN, Index], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([LocalVN, RemoteVN, Index]) ->
    State = #state{local=LocalVN,
                   remote=RemoteVN,
                   index=Index,
                   built=0},
    {ok, update_trees, State, 0}.

update_trees(timeout, State=#state{local=LocalVN,
                                   remote=RemoteVN,
                                   index=Index}) ->
    lager:info("Sending to ~p", [LocalVN]),
    lager:info("Sending to ~p", [RemoteVN]),

    Sender = {fsm, undefined, self()},
    riak_core_vnode_master:command(LocalVN,
                                   {update_tree, Index},
                                   Sender,
                                   riak_kv_vnode_master),
    riak_core_vnode_master:command(RemoteVN,
                                   {update_tree, Index},
                                   Sender,
                                   riak_kv_vnode_master),
    {next_state, update_trees, State};
update_trees({not_responsible, VNodeIdx, Index}, State) ->
    lager:info("VNode ~p does not cover index ~p", [VNodeIdx, Index]),
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
            {next_state, update_trees, State#state{built=Built}}
    end.

key_exchange(timeout, State=#state{local=LocalVN,
                                   remote=RemoteVN,
                                   index=Index}) ->
    lager:info("Starting key exchange between ~p and ~p", [LocalVN, RemoteVN]),
    lager:info("Exchanging hashes for preflist ~p", [Index]),
    %% R1 = riak_core_vnode_master:sync_command(LocalVN,
    %%                                          {exchange_bucket, Index, 1, 0},
    %%                                          riak_kv_vnode_master),
    %% R2 = riak_core_vnode_master:sync_command(RemoteVN,
    %%                                          {exchange_bucket, Index, 1, 0},
    %%                                          riak_kv_vnode_master),


    %% lager:info("R1: ~p", [R1]),
    %% lager:info("R2: ~p", [R2]),

    Remote = fun(get_bucket, {L, B}) ->
                     exchange_bucket(RemoteVN, Index, L, B);
                (key_hashes, Segment) ->
                     exchange_segment(RemoteVN, Index, Segment)
             end,

    R = riak_core_vnode_master:sync_command(LocalVN,
                                            {compare_trees, Index, Remote},
                                            riak_kv_vnode_master),

    {keydiff,_,_,KeyDiff} = R,
    Missing = [binary_to_term(BKey) || {_, BKey} <- KeyDiff],
    {ok, RC} = riak:local_client(),
    [begin
         lager:info("Anti-entropy forced read repair: ~p/~p", [Bucket, Key]),
         RC:get(Bucket, Key)
     end || {Bucket, Key} <- Missing],
    {stop, normal, State}.

exchange_bucket(VN, Index, Level, Bucket) ->
    riak_core_vnode_master:sync_command(VN,
                                        {exchange_bucket, Index, Level, Bucket},
                                        riak_kv_vnode_master).

exchange_segment(VN, Index, Segment) ->
    riak_core_vnode_master:sync_command(VN,
                                        {exchange_segment, Index, Segment},
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
