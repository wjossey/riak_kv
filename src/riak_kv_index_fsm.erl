%% -------------------------------------------------------------------
%%
%% riak_index_fsm: Manage secondary index queries.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc The index fsm manages the execution of secondary index queries.
%%
%%      The index fsm creates a plan to achieve coverage
%%      of the cluster using the minimum
%%      possible number of VNodes, sends index query
%%      commands to each of those VNodes, and compiles the
%%      responses.
%%
%%      The number of VNodes required for full
%%      coverage is based on the number
%%      of partitions, the number of available physical
%%      nodes, and the bucket n_val.

-module(riak_kv_index_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("riak_kv_vnode.hrl").

-export([init/2,
         plan/2,
         process_results/2,
         finish/2]).

-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {client_type :: plain | mapred,
                max_results=100,
                results_so_far=0,
                merge_sort_buffer :: term(),
                from :: from()}).

%% @doc Return a tuple containing the ModFun to call per vnode,
%% the number of primary preflist vnodes the operation
%% should cover, the service to use to check for available nodes,
%% and the registered name to use to access the vnode master process.
init(From={_, _, ClientPid}, [Bucket, ItemFilter, Query, Timeout, ClientType]) ->
    case ClientType of
        %% Link to the mapred job so we die if the job dies
        mapred ->
            link(ClientPid);
        _ ->
            ok
    end,
    %% Get the bucket n_val for use in creating a coverage plan
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    NVal = proplists:get_value(n_val, BucketProps),
    %% Construct the key listing request
    Req = ?KV_INDEX_REQ{bucket=Bucket,
                        item_filter=ItemFilter,
                        qry=Query},
    {Req, all, NVal, 1, riak_kv, riak_kv_vnode_master, Timeout,
     #state{client_type=ClientType, from=From}}.

plan(CoverageVnodes, State) ->
    State2 = State#state{merge_sort_buffer=sms:new(CoverageVnodes)},
    {ok, State2}.

process_results({error, Reason}, _State) ->
    {error, Reason};
process_results({_Vnode, {_Bucket, _Results}},
                StateData=#state{results_so_far=ResultsSoFar,
                                 max_results=ResultsSoFar}) ->
    lager:debug("Got more results but already sent enough down the wire"),
    {ok, StateData};
process_results({Vnode, {_Bucket, Results}},
                StateData=#state{client_type=_ClientType,
                                 merge_sort_buffer=MergeSortBuffer,
                                 max_results=MaxResults,
                                 results_so_far=ResultsSoFar,
                                 from={raw, ReqId, ClientPid}}) ->
    %% TODO: this isn't compatible with mapreduce

    ReversedResults = lists:reverse(Results),

    %% add new results to buffer
    BufferWithNewResults = sms:add_results(Vnode, ReversedResults, MergeSortBuffer),
    ProcessBuffer = sms:sms(BufferWithNewResults),
    {NewBuffer, LengthSent} = case ProcessBuffer of
        {[], BufferWithNewResults} ->
            {BufferWithNewResults, 0};
        {ToSend, NewBuff} ->
            DownTheWire = case (ResultsSoFar + length(ToSend)) > MaxResults of
                true ->
                    lists:sublist(ToSend, ((ResultsSoFar + length(ToSend)) - MaxResults));
                false ->
                    ToSend
            end,
            ClientPid ! {ReqId, {results, DownTheWire}},
            {NewBuff, length(DownTheWire)}
    end,
    {ok, StateData#state{merge_sort_buffer=NewBuffer,
                         results_so_far=(ResultsSoFar + LengthSent)}};
process_results({_VnodeID, done}, StateData) ->
    {done, StateData}.

finish({error, Error},
       StateData=#state{from={raw, ReqId, ClientPid},
                        client_type=ClientType}) ->
    case ClientType of
        mapred ->
            %% An error occurred or the timeout interval elapsed
            %% so all we can do now is die so that the rest of the
            %% MapReduce processes will also die and be cleaned up.
            exit(Error);
        plain ->
            %% Notify the requesting client that an error
            %% occurred or the timeout has elapsed.
            ClientPid ! {ReqId, {error, Error}}
    end,
    {stop, normal, StateData};
finish(clean,
       StateData=#state{from={raw, ReqId, ClientPid},
                        results_so_far=ResultsSoFar,
                        max_results=MaxResults,
                        merge_sort_buffer=MergeSortBuffer,
                        client_type=ClientType}) ->
    case ClientType of
        mapred ->
            luke_flow:finish_inputs(ClientPid);
        plain ->
            LastResults = sms:done(MergeSortBuffer),
            DownTheWire = case (ResultsSoFar + length(LastResults)) > MaxResults of
                true ->
                    lists:sublist(LastResults, ((ResultsSoFar + length(LastResults)) - MaxResults));
                false ->
                    LastResults
            end,
            lager:debug("Sent ~p results total", 
                [ResultsSoFar + length(DownTheWire)]),
            ClientPid ! {ReqId, {results, DownTheWire}},
            ClientPid ! {ReqId, done}
    end,
    {stop, normal, StateData}.
