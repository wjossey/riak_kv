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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([LocalVN, RemoteVN, Index]) ->
    State = #state{local=LocalVN,
                   remote=RemoteVN,
                   index=Index,
                   built=0},
    {ok, update_trees, State, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
%% state_name(_Event, _From, State) ->
%%     Reply = ok,
%%     {reply, Reply, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, _StateName, State) ->
    %% {next_state, StateName, State}.
    {stop, badmsg, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, _StateName, State) ->
    %% {reply, ok, StateName, State}.
    {stop, badmsg, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, _StateName, State) ->
    %% {next_state, StateName, State}.
    {stop, badmsg, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
