-module(fsmlog).
-behaviour(gen_server).

%% API
-export([run/0, run/1]).
-export([start_link/0, start/0, stop/0]).
-export([trace_fsms/0, report_fsms/0, log_fsms/1, log_vnodes/1, log_fsm_stats/1,
         timestamp/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {local_epoch, %% Local gregorian seconds at 1/1/1970
                fsm_fh,
                fsm_stats_fh,
                vnode_fh,
                fsms_tref,
                fsms_interval,
                vnode_tref,
                vnode_interval,
                vnode_idxs = [], % { Pid, VnodeIdx}
                get_fsm_5min = new_fsm_hist(),
                put_fsm_5min = new_fsm_hist()
               }).



%%%===================================================================
%%% API
%%%===================================================================
run() ->
    run("/tmp/riak").

run(LogDir) ->
    {ok, _Pid} = start(),
    trace_fsms(),
    log_fsms(filename:join(LogDir, "fsm_trace.log.gz")),
    log_fsm_stats(filename:join(LogDir, "fsm_stats.log")),
    log_vnodes(filename:join(LogDir, "vnodes.log.gz")),
    ok.
    
start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop).

trace_fsms() ->
    trace_fsms(timer:minutes(5)).

trace_fsms(Interval) ->
    gen_server:call(?MODULE, {trace_fsms, Interval}).

report_fsms() ->
    gen_server:call(?MODULE, report_fsms).

log_fsms(Filename) ->
    gen_server:call(?MODULE, {log_fsms, Filename}).

log_fsm_stats(Filename) ->
    gen_server:call(?MODULE, {log_fsm_stats, Filename}).

log_vnodes(Filename) ->
    log_vnodes(Filename, timer:seconds(5)).

log_vnodes(Filename, Interval) ->
    gen_server:call(?MODULE, {log_vnodes, Filename, Interval}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {0, TimeDiff} = calendar:time_difference(calendar:local_time(), calendar:universal_time()),
    TimeDiffSecs = calendar:time_to_seconds(TimeDiff),
    Epoch = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}) - TimeDiffSecs,
    {ok, #state{local_epoch = Epoch}}.

handle_call({trace_fsms, Interval}, _From, State) ->
    State1 = schedule_reset_fsm_hists(State#state{fsms_interval = Interval}),
    dbg:stop_clear(),
    dbg:tracer(process, {fun stat_trace/2, []}),
    dbg:p(all, call),
    dbg:tpl(riak_kv_stat, update, 3, []),
    {reply, ok, State1};
handle_call(report_fsms, _From, State = #state{get_fsm_5min = Get5,
                                               put_fsm_5min = Put5}) ->
    Reply = [hist_summary(get5, Get5),
             hist_summary(put5, Put5)],
    {reply, Reply, State};
handle_call({log_fsms, Filename}, _From, State = #state{fsm_fh = OldFh}) ->
    catch file:close(OldFh),
    case open(Filename, [write, raw, binary, delayed_write]) of
        {ok, Fh} ->
            {reply, ok, State#state{fsm_fh = Fh}};
        ER ->
            error_logger:error_msg("~p: Could not open FSM trace log ~p: ~p",
                                   [?MODULE, Filename, ER]),
            {reply, ER, State#state{fsm_fh = undefined}}
    end;
handle_call({log_vnodes, Filename, Interval}, _From, State = #state{vnode_fh = OldFh}) ->
    State1 = schedule_log_vnodes(State#state{vnode_interval = Interval}),
    catch file:close(OldFh),
    case open(Filename, [write, raw, binary]) of
        {ok, Fh} ->
            {reply, ok, State1#state{vnode_fh = Fh}};
        ER ->
            error_logger:error_msg("~p: Could not open vnode stats log ~p: ~p",
                                   [?MODULE, Filename, ER]),
            {reply, ER, State1#state{vnode_fh = undefined}}
    end;
handle_call({log_fsm_stats, Filename}, _From, State = #state{fsm_stats_fh = OldFh}) ->
    catch file:close(OldFh),
    case open(Filename, [write, raw, binary]) of
        {ok, Fh} ->
            {reply, ok, State#state{fsm_stats_fh = Fh}};
        ER ->
            error_logger:error_msg("~p: Could not open FSM stats log ~p: ~p",
                                   [?MODULE, Filename, ER]),
            {reply, ER, State#state{fsm_stats_fh = undefined}}
    end;
handle_call(stop, _From, State) ->
    dbg:stop_clear(),
    {stop, normal, ok, State}.


handle_cast(_Msg, State) ->
    {stop, {unexpected, _Msg}, State}.

handle_info({get_fsm_usecs, Moment, Usecs}, State = #state{get_fsm_5min = FiveMinHist}) ->
    State1 = trace_log_fsm(get_fsm, Moment, Usecs, State),
    {noreply, State1#state{get_fsm_5min = basho_stats_histogram:update(Usecs, FiveMinHist)}};
handle_info({put_fsm_usecs, Moment, Usecs}, State = #state{put_fsm_5min = FiveMinHist}) ->
    State1 = trace_log_fsm(put_fsm, Moment, Usecs, State),
    {noreply, State1#state{put_fsm_5min = basho_stats_histogram:update(Usecs, FiveMinHist)}};

handle_info(reset_fsm_hists, State = #state{fsm_stats_fh = Fh,
                                            get_fsm_5min = Get5,
                                            put_fsm_5min = Put5}) ->
    State1 = schedule_reset_fsm_hists(State),
    TS = timestamp(),
    log_vnode_stats(TS, get, Get5, Fh),
    log_vnode_stats(TS, put, Put5, Fh),
    {noreply, State1#state{get_fsm_5min = new_fsm_hist(),
                          put_fsm_5min = new_fsm_hist()}};

handle_info(log_vnodes, State = #state{vnode_fh = Fh}) ->
    State1 = schedule_log_vnodes(State),
    Pids = get_vnode_pids(),
    TS = timestamp(),
    Prefix = integer_to_list(TS),
    {VnodeInfo, State2} = lists:foldl(fun make_vnode_entry/2, {[], State1}, Pids),
    Entries = [ [Prefix, $,, integer_to_list(VIdx), $,, integer_to_list(MsgQ), $\n] || 
                  {VIdx, MsgQ} <- lists:sort(VnodeInfo) ],
    case file:write(Fh, Entries) of
        ok ->
            {noreply, State2};
        ER ->
            error_logger:error_msg("~p: Could not log vnodes: ~p\n", [?MODULE, ER]),
            {noreply, State1}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

timestamp() ->
    {Mega, Sec, _} = now(),
    (Mega * 1000000) + Sec.


hist_summary(What, Hist) ->
    {Min, Mean, Max, _, _} = basho_stats_histogram:summary_stats(Hist),
    [{What, obs, basho_stats_histogram:observations(Hist)},
     {What, min, maybe_trunc(Min)},
     {What, mean, maybe_trunc(Mean)},
     {What, max, maybe_trunc(Max)}] ++
        [{What, Pctl, maybe_trunc(basho_stats_histogram:quantile(Pctl, Hist))} || 
            Pctl <- [0.500, 0.950, 0.990, 0.999]].

log_vnode_stats(TS, What, Hist, Fh) ->
    {Min, Mean, Max, _, _} = basho_stats_histogram:summary_stats(Hist),
    Data = [TS, 
            What, 
            basho_stats_histogram:observations(Hist),
            maybe_trunc(Min),
            maybe_trunc(Mean),
            maybe_trunc(Max)] ++
        [maybe_trunc(basho_stats_histogram:quantile(Pctl, Hist)) ||
            Pctl <- [0.500, 0.950, 0.990, 0.999]],
    output_csv(Data, Fh).

output_csv(Cols, Fh) ->
    Out = [io_lib:format("~p", [hd(Cols)]) | [io_lib:format(",~p", [Col]) || Col <- tl(Cols)]],
    file:write(Fh, [Out, $\n]).
     

maybe_trunc('NaN') ->
    0;
maybe_trunc(Val) ->
    try
        trunc(Val)
    catch _:_ ->
            Val
    end.

trace_log_fsm(_Op, _Moment, _Usecs, State = #state{fsm_fh = undefined}) ->
    State;
trace_log_fsm(Op, Moment, Usecs, State = #state{fsm_fh = Fh, local_epoch = Epoch}) ->
    TS = Moment - Epoch,
    Entry = io_lib:format("~b,~p,~b\n", [TS, Op, Usecs]),
    case file:write(Fh, Entry) of
        ok ->
            State;
        ER ->
            error_logger:error_msg("Could not write entry - ~p.  Disabling FSM log to disk.\n",
                                   [ER]),
            catch file:close(Fh),
            State#state{fsm_fh = undefined}
    end.

make_vnode_entry(Pid, {Entries, State}) ->
    try
        {VIdx, State1} = find_vidx(Pid, State),
        {message_queue_len, MsgQ} = erlang:process_info(Pid, message_queue_len),
        {[{VIdx, MsgQ} | Entries], State1}
    catch _:Err ->
            error_logger:error_msg("~p: could not log vnode msgq: ~p\n", [?MODULE, Err]),
            {Entries,  State}
    end.


find_vidx(Pid, State = #state{vnode_idxs = VIdxs}) ->
    case orddict:find(Pid, VIdxs) of
        {ok, VIdxStr} ->
            {VIdxStr, State};
        error ->
            {_Mod, Vnode} = riak_core_vnode:get_mod_index(Pid),
            VIdx = vnode_to_vidx(Vnode),
            {VIdx, State#state{vnode_idxs = orddict:store(Pid, VIdx, VIdxs)}}
    end.

vnode_to_vidx(Vnode) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Q = riak_core_ring:num_partitions(R),
    Wedge = trunc(math:pow(2,160)) div Q, % how much keyspace for each vnode
    Vnode div Wedge. % convert to small integer from 0..Q-1

get_vnode_pids() ->
    [Pid || {undefined, Pid, worker, dynamic} <- supervisor:which_children(riak_core_vnode_sup)].

stat_trace({trace, _Pid, call, {_Mod, _Fun, [{get_fsm_time, Usecs}, Moment, _State]}}, Acc) ->
%    io:format(user, "get FSM: ~p\n", [Usecs]),
    ?MODULE ! {get_fsm_usecs, Moment, Usecs},
    Acc;
stat_trace({trace, _Pid, call, {_Mod, _Fun, [{put_fsm_time, Usecs}, Moment, _State]}}, Acc) ->
%    io:format(user, "put FSM: ~p\n", [Usecs]),
    ?MODULE ! {put_fsm_usecs, Moment, Usecs},
    Acc;
stat_trace({trace, _Pid, call, {_Mod, _Fun, _Args}}, Acc) ->
    %% io:format(user, "missed: ~p\n", [{_Mod, _Fun, _Args}]),
    Acc.

new_fsm_hist() ->
    %% Tracks latencies up to 5 secs w/ 250 us resolution
    basho_stats_histogram:new(0, 5000000, 20000).
  
schedule_reset_fsm_hists(State = #state{fsms_tref = OldTref, fsms_interval = Interval}) ->
    maybe_cancel_timer(OldTref),
    Tref = erlang:send_after(Interval, self(), reset_fsm_hists),
    State#state{fsms_tref = Tref}.

schedule_log_vnodes(State = #state{vnode_tref = OldTref, vnode_interval = Interval}) ->
    maybe_cancel_timer(OldTref),
    Tref = erlang:send_after(Interval, self(), log_vnodes),
    State#state{vnode_tref = Tref}.

maybe_cancel_timer(undefined) ->
    ok;
maybe_cancel_timer(Tref) ->
    erlang:cancel_timer(Tref).

open(Filename, Options) ->
    case filename:extension(Filename) of
        ".gz" ->
            file:open(Filename, [compressed | Options]);
        _ ->
            file:open(Filename, Options)
    end.
               
%% -module(logfsms).

%% timeit(Mod, Fun, Arity) ->
%%     dbg:tracer(process, {fun trace/2, []}),
%%     dbg:p(all, call),
%%     dbg:tpl(Mod, Fun, Arity, [{'_', [], [{return_trace}]}]).




%% trace({trace, Pid, call, {Mod, Fun, _}}, Acc) ->
%%     orddict:store({Pid, Mod, Fun}, now(), Acc);
%% trace({trace, Pid, return_from, {Mod, Fun, _}, _Result}, Acc) ->
%%     case orddict:find({Pid, Mod, Fun}, Acc) of
%%         {ok, StartTime} ->
%%             ElapsedUs = timer:now_diff(now(), StartTime),
%%             io:format(user, "~p:~p:~p: ~p us\n", [Pid, Mod, Fun, ElapsedUs]),
%%             Acc;
%%         error ->
%%             Acc
%%     end.


%% %% do_put, do_put
