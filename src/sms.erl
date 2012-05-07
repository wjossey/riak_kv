-module(sms).

-define(DICTMODULE, orddict).

-export([new/1,
         add_results/3,
         done/1,
         sms/1]).

new(Vnodes) ->
    DictList = [{VnodeID, []} || VnodeID <- Vnodes],
    ?DICTMODULE:from_list(DictList).

add_results(VnodeID, Results, Data) ->
    UpdateFun = fun (Prev) -> Prev ++ Results end,
    update(VnodeID, UpdateFun, Data).

update(VnodeID, UpdateFun, Data) ->
    ?DICTMODULE:update(VnodeID, UpdateFun, Data).

done(Data) ->
    lists:merge(values(Data)).

sms(Data) ->
    case any_empty(values(Data)) of
        true ->
            {[], Data};
        false ->
            unsafe_sms(Data)
    end.

unsafe_sms(Data) ->
    MinOfLastsOfLists = lists:min([lists:last(List) || List <- values(Data)]),
    SplitFun = fun (Elem) -> Elem =< MinOfLastsOfLists end,
    Split = ?DICTMODULE:map(fun (_Key, V) -> lists:splitwith(SplitFun, V) end, Data),
    %%SplitLists = [lists:splitwith(SplitFun, List) || List <- ListOfLists],
    LessThan = ?DICTMODULE:map(fun (_Key, V) -> element(1, V) end, Split),
    %%LessThan = [element(1, Tuple) || Tuple <- SplitLists],
    GreaterThan = ?DICTMODULE:map(fun (_Key, V) -> element(2, V) end, Split),
    %%GreaterThan = [element(2, Tuple) || Tuple <- SplitLists],
    Merged = lists:merge(values(LessThan)),
    {Merged, GreaterThan}.

values(Data) ->
    [V || {_Key, V} <- ?DICTMODULE:to_list(Data)].

empty([]) -> true;
empty(_) -> false.

any_empty(Lists) ->
    lists:any(fun empty/1, Lists).
