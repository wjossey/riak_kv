-module(entropy_manager).
-compile(export_all).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {trees,
                tree_queue}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_tree(Index, Pid) ->
    gen_server:call(?MODULE, {register_tree, Index, Pid}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    schedule_tick(),
    {ok, #state{trees=[],
                tree_queue=[]}}.

handle_call({register_tree, Index, Pid}, _From, State) ->
    State2 = do_register_tree(Index, Pid, State),
    {reply, ok, State2};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = tick(State),
    {noreply, State2};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_register_tree(Index, Pid, State=#state{trees=Trees}) ->
    Trees2 = orddict:store(Index, Pid, Trees),
    State#state{trees=Trees2}.

next_tree(#state{trees=[]}) ->
    throw(no_trees_registered);
next_tree(State=#state{tree_queue=[], trees=Trees}) ->
    State2 = State#state{tree_queue=Trees},
    next_tree(State2);
next_tree(State=#state{tree_queue=Queue}) ->
    %% TODO: Handle dead pids and remove them
    [{_Index,Pid}|Rest] = Queue,
    State2 = State#state{tree_queue=Rest},
    {Pid, State2}.

schedule_tick() ->
    %% Tick = app_helper:get_env(riak_core,
    %%                           claimant_tick,
    %%                           10000),
    Tick = 1000,
    erlang:send_after(Tick, ?MODULE, tick).

tick(State) ->
    State2 = lists:foldl(fun(_,S) ->
                                 maybe_poke_tree(S)
                         end, State, lists:seq(1,10)),
    schedule_tick(),
    State2.

maybe_poke_tree(State=#state{trees=[]}) ->
    State;
maybe_poke_tree(State) ->
    {Tree, State2} = next_tree(State),
    Tree ! tick,
    State2.

