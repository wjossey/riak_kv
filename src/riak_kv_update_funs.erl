%% -------------------------------------------------------------------
%%
%% riak_kv_update_funs
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

%% @doc Useful built-in update functions.
-module(riak_kv_update_funs).
-include_lib("riak_kv_vnode.hrl").
-export([update_counter/3, 
         update_counter/4,
         add_measurement/3, 
         add_measurement/4]).
-export([update_counter_v/4,
         add_measurement_v/4]).


%% client helpers
update_counter(Client, BKey, IncBy) ->
    update_counter(Client, BKey, IncBy, [{returnbody, true}]).    
update_counter(Client, {Bucket, Key}, IncBy, Options) ->
    Client:update({Bucket, Key},{?MODULE,update_counter_v,[IncBy]},Options).

%% vnode funs
update_counter_v({Bucket, Key}, {error, notfound}, IncBy, _Ctx) ->
    {ok, riak_object:new(Bucket, Key, IncBy)};
update_counter_v({_, _}, Obj, IncBy, _Ctx) ->
    Val = riak_object:get_value(Obj),
    {ok,riak_object:update_value(Obj, Val+IncBy)}.

%% client helpers
add_measurement(Client, BKey, {Time, Value}) ->
    add_measurement(Client, BKey, {Time, Value}, [{returnbody, true}]).
add_measurement(Client, {Bucket, Key}, {Time, Value}, Options) ->
    Client:update({Bucket, Key},{?MODULE,add_measurement_v,[{Time,Value}]},
                  Options).

%% vnode funs
add_measurement_v({Bucket, Key}, {error, notfound}, {Time,Value}, _Ctx) ->
    Series = [{Time, Value}],
    Stats = calc_series_stats(Series),
    {ok, riak_object:new(Bucket, Key, {Series, Stats})};
add_measurement_v({_, _}, Obj, {Time, Value}, _Ctx) ->
    {OldSeries, _OldStats} = riak_object:get_value(Obj),
    NewSeries = [{Time, Value}|OldSeries],
    NewStats = calc_series_stats(NewSeries),
    {ok, riak_object:update_value(Obj, {NewSeries, NewStats})}.

calc_series_stats(Series) ->
    Vals = [V || {_T, V} <- Series],
    [{min, lists:min(Vals)},
     {max, lists:max(Vals)},
     {sum, lists:sum(Vals)},
     {avg, lists:sum(Vals) / length(Vals)}].
    

