%%%----------------------------------------------------------------------
%%% File    : test_driver_test_fsm.erl
%%% Purpose : Test FSM module for testing the gen_fsm_test_driver hack
%%%----------------------------------------------------------------------

-module(test_driver_test_fsm).

-behaviour(gen_fsm).

-define(DRIVER, gen_fsm_test_driver).

%% External exports
-export([start/2, pure_start/2]).

%% gen_fsm callbacks
-export([init/1, 
         state_1/2, state_1/3,
         state_2/2, state_2/3,
         handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3,
         code_change/4]).

%% Testing API with gen_fsm_test_driver
-export([t0/0]).

-record(state, {
          arg1,
          arg2,
          options
         }).

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------

start(Mod, Args) ->
    gen_fsm:start(Mod, Args, []).

pure_start(FsmID, Args) ->
    ?DRIVER:start(FsmID, ?MODULE, Args).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_fsm
%%%----------------------------------------------------------------------

%%----------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, StateName, StateData}          |
%%          {ok, StateName, StateData, Timeout} |
%%          ignore                              |
%%          {stop, StopReason}                   
%%----------------------------------------------------------------------
init({X, Y, Options}) ->
    {ok, state_1, #state{arg1 = X,
                         arg2 = Y,
                         options = Options}, 0}.

%%----------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                         
%%----------------------------------------------------------------------
state_1(timeout = Event, StateData) ->
    gen_fsm_test_driver:add_trace(StateData#state.options,
                                  {state_1, Event}),
    {next_state, state_2, StateData};
state_1(all_done = Event, StateData) ->
    gen_fsm_test_driver:add_trace(StateData#state.options,
                                  {state_1, Event}),
    {stop, normal, StateData}.

%%----------------------------------------------------------------------
%% Func: StateName/3
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}                    
%%----------------------------------------------------------------------
state_1(Event, From, StateData) ->
    Reply = ok_state1,
    gen_fsm_test_driver:add_trace(StateData#state.options,
                                  {state_1, Event, from, From, reply, Reply}),
    {reply, Reply, state_1, StateData}.

%%----------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                         
%%----------------------------------------------------------------------
state_2(Event, StateData) ->
    gen_fsm_test_driver:add_trace(StateData#state.options,
                                  {state_2, Event}),
    {next_state, state_2, StateData}.

%%----------------------------------------------------------------------
%% Func: StateName/3
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}                    
%%----------------------------------------------------------------------
state_2(Event, From, StateData) ->
    Reply = ok,
    gen_fsm_test_driver:add_trace(StateData#state.options,
                                  {state_2, Event, from, From, reply, Reply}),
    {reply, Reply, state_1, StateData}.

%%----------------------------------------------------------------------
%% Func: handle_event/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                         
%%----------------------------------------------------------------------
handle_event(Event, StateName, StateData) ->
    gen_fsm_test_driver:add_trace(StateData#state.options,
                                  {all_state_event, Event, state_name, StateName}),
    {next_state, StateName, StateData}.

%%----------------------------------------------------------------------
%% Func: handle_sync_event/4
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}                    
%%----------------------------------------------------------------------
handle_sync_event(Event, From, StateName, StateData) ->
    Reply = ok,
    gen_fsm_test_driver:add_trace(StateData#state.options,
                                  {sync_event, Event, from, From, state_name, StateName, reply, Reply}),
    {reply, Reply, StateName, StateData}.

%%----------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                         
%%----------------------------------------------------------------------
handle_info(Info, StateName, StateData) ->
    gen_fsm_test_driver:add_trace(StateData#state.options,
                                  {info, Info, state_name, StateName}),
    {next_state, StateName, StateData}.

%%----------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%%----------------------------------------------------------------------
terminate(_Reason, _StateName, _StatData) ->
    ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

%%%----------------------------------------------------------------------
%%% Testing functions
%%%----------------------------------------------------------------------

t0() ->
    Ref0 = 0,
    InitIter = ?MODULE:pure_start(Ref0, {foo, bar, [{my_ref, Ref0}]}),
    %% Try one of everything?
    Events = [{send_event, should_be_in_state_2_at_receive},
              {send_all_state_event, should_be_in_state_2_again},
              {sync_send_event, sync_client_1, still_in_state_2},
              {sync_send_all_state_event, sync_client_2, back_in_1},
              {send_event, all_done}
             ],
    X = ?DRIVER:run_to_completion(Ref0, ?MODULE, InitIter, Events),
    [{res, X},
     {state, ?DRIVER:get_state(Ref0)},
     {statedata, ?DRIVER:get_statedata(Ref0)},
     {trace, ?DRIVER:get_trace(Ref0)}].
