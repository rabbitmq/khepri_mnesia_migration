%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @private

-module(m2k_cluster_sync).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("src/kmm_error.hrl").
-include("src/kmm_logging.hrl").

-export([start_link/1,
         proceed/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-ifdef(TEST).
-export([sort_khepri_clusters/2]).
-endif.

-record(?MODULE, {khepri_store}).

proceed(Pid) ->
    case gen_server:call(Pid, ?FUNCTION_NAME, infinity) of
        {exception, ?kmm_exception(_, _) = Exception} ->
            ?kmm_misuse(Exception);
        Ret ->
            Ret
    end.

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% -------------------------------------------------------------------
%% `gen_server' callbacks.
%% -------------------------------------------------------------------

init(#{khepri_store := StoreId}) ->
    erlang:process_flag(trap_exit, true),
    State = #?MODULE{khepri_store = StoreId},
    {ok, State}.

handle_call(proceed, _From, State) ->
    Ret = try
              do_sync_cluster(State)
          catch
              throw:?kmm_error(_, _) = Reason ->
                  ?LOG_ERROR(
                     "Failed to synchronize Mnesia->Khepri clusters: ~0p",
                     [Reason],
                     #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),
                  {error, Reason};
              error:?kmm_exception(_, _) = Exception ->
                  ?LOG_ERROR(
                     "Exception during Mnesia->Khepri clusters sync: ~0p",
                     [Exception],
                     #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),
                  {exception, Exception}
          end,
    {reply, Ret, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_call message: ~p",
       [Request],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),
    {reply, undefined, State}.

handle_cast(Request, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_cast message: ~p",
       [Request],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_info message: ~p",
       [Msg],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

do_sync_cluster(#?MODULE{khepri_store = StoreId} = State) ->
    Lock = {{?MODULE, StoreId}, self()},
    global:set_lock(Lock),
    try
        do_sync_cluster_locked(State)
    after
        global:del_lock(Lock)
    end.

do_sync_cluster_locked(#?MODULE{khepri_store = StoreId}) ->
    ?LOG_INFO(
       "Syncing Mnesia->Khepri clusters membership",
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    MnesiaCluster = kmm_utils:mnesia_nodes(),
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Mnesia cluster: ~0p",
       [MnesiaCluster],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    LargestKhepriCluster = find_largest_khepri_cluster(MnesiaCluster, StoreId),
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Largest Khepri cluster: ~0p",
       [LargestKhepriCluster],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    NodesToAdd = MnesiaCluster -- LargestKhepriCluster,
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Khepri nodes joining the largest "
       "Khepri cluster: ~0p",
       [NodesToAdd],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    add_nodes_to_khepri_cluster(NodesToAdd, LargestKhepriCluster, StoreId),

    KhepriCluster = khepri_cluster_on_node(hd(LargestKhepriCluster), StoreId),
    NodesToRemove = KhepriCluster -- MnesiaCluster,
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Khepri nodes being removed from the "
       "expanded Khepri cluster: ~0p",
       [NodesToRemove],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    remove_nodes_from_khepri_cluster(NodesToRemove, StoreId).

find_largest_khepri_cluster(Nodes, StoreId) ->
    KhepriClusters0 = list_all_khepri_clusters(Nodes, StoreId),
    KhepriClusters1 = remove_khepri_nodes_not_in_mnesia_cluster(
                        Nodes, KhepriClusters0),
    SortedKhepriClusters = sort_khepri_clusters(KhepriClusters1, StoreId),
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Khepri clusters: ~0p",
       [SortedKhepriClusters],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),
    LargestKhepriCluster = hd(SortedKhepriClusters),
    LargestKhepriCluster.

list_all_khepri_clusters(Nodes, StoreId) ->
    KhepriClusters = lists:foldl(
                       fun(Node, Acc) ->
                               Cluster = khepri_cluster_on_node(Node, StoreId),
                               Acc#{Cluster => true}
                       end, #{}, Nodes),
    maps:keys(KhepriClusters).

khepri_cluster_on_node(Node, StoreId) ->
    case rpc:call(Node, khepri_cluster, nodes, [StoreId]) of
        {ok, AllNodes} when is_list(AllNodes) andalso AllNodes =/= [] ->
            SortedNodes = lists:sort(AllNodes),
            SortedNodes;
        {error, noproc} ->
            ?kmm_misuse(
               khepri_store_must_run,
               #{node => Node,
                 store_id => StoreId});
        {error, _Reason} = Error ->
            throw(
              ?kmm_error(
                 failed_to_query_khepri_nodes,
                 #{node => Node,
                   store_id => StoreId,
                   error => Error}))
    end.

remove_khepri_nodes_not_in_mnesia_cluster(MnesiaCluster, KhepriClusters) ->
    %% We need to leave Khepri nodes that are not part of the Mnesia cluster
    %% alone.
    lists:filtermap(
      fun(KhepriCluster) ->
              KhepriCluster1 = remove_khepri_nodes_not_in_mnesia_cluster1(
                                 MnesiaCluster, KhepriCluster),
              case KhepriCluster1 of
                  [] -> false;
                  _  -> {true, KhepriCluster1}
              end
      end, KhepriClusters).

remove_khepri_nodes_not_in_mnesia_cluster1(MnesiaCluster, KhepriCluster) ->
    lists:filter(
      fun(KhepriNode) ->
              lists:member(KhepriNode, MnesiaCluster)
      end, KhepriCluster).

-define(TREE_NODES_COUNTS_KEY, kmm_tree_nodes_counts).
-define(ERLANG_NODES_UPTIMES_KEY, kmm_erlang_node_uptimes).

sort_khepri_clusters(KhepriClusters, StoreId) ->
    _ = erlang:put(?TREE_NODES_COUNTS_KEY, #{}),
    _ = erlang:put(?ERLANG_NODES_UPTIMES_KEY, #{}),
    SortedNodes = do_sort_khepri_clusters_by_size(KhepriClusters, StoreId),
    _ = erlang:erase(?ERLANG_NODES_UPTIMES_KEY),
    _ = erlang:erase(?TREE_NODES_COUNTS_KEY),
    SortedNodes.

do_sort_khepri_clusters_by_size(KhepriClusters, StoreId) ->
    Criterias = [fun compare_members_count/3,
                 fun compare_tree_nodes_count/3,
                 fun compare_erlang_node_uptimes/3,
                 fun compare_erlang_node_names/3],
    lists:sort(
      fun(A, B) ->
              Sort = lists:foldl(
                       fun
                           (Criteria, undefined) -> Criteria(A, B, StoreId);
                           (_Criteria, Result)   -> Result
                       end, undefined, Criterias),
              ?assertNotEqual(undefined, Sort),
              Sort
      end,
      KhepriClusters).

compare_members_count(A, B, _StoreId) ->
    AMembersCount = length(A),
    BMembersCount = length(B),
    if
        AMembersCount =:= BMembersCount -> undefined;
        true                            -> length(A) > length(B)
    end.

compare_tree_nodes_count(A, B, StoreId) ->
    ANodesCount = get_tree_nodes_count(A, StoreId),
    BNodesCount = get_tree_nodes_count(B, StoreId),
    if
        ANodesCount =:= BNodesCount -> undefined;
        true                        -> ANodesCount > BNodesCount
    end.

compare_erlang_node_uptimes(A, B, _StoreId) ->
    ALongestUptime = get_longest_erlang_node_uptime(A),
    BLongestUptime = get_longest_erlang_node_uptime(B),
    if
        ALongestUptime =:= BLongestUptime -> undefined;
        true                              -> ALongestUptime > BLongestUptime
    end.

compare_erlang_node_names(A, B, _StoreId) ->
    A =< B.

get_tree_nodes_count(Nodes, StoreId) ->
    TreeNodesCounts = erlang:get(?TREE_NODES_COUNTS_KEY),
    case TreeNodesCounts of
        #{Nodes := TreeNodesCount} ->
            TreeNodesCount;
        _ ->
            Node = hd(Nodes),
            Ret = rpc:call(Node, khepri, count, [StoreId, "**"]),
            case Ret of
                {ok, TreeNodesCount} ->
                    TreeNodesCounts1 = TreeNodesCounts#{
                                         Nodes => TreeNodesCount},
                    _ = erlang:put(?TREE_NODES_COUNTS_KEY, TreeNodesCounts1),
                    TreeNodesCount;
                Error ->
                    throw(
                      ?kmm_error(
                         failed_to_query_khepri_tree_nodes_count,
                         #{node => Node,
                           store_id => StoreId,
                           error => Error}))
            end
    end.

get_longest_erlang_node_uptime(Nodes) ->
    NodeUptimes = erlang:get(?ERLANG_NODES_UPTIMES_KEY),
    case NodeUptimes of
        #{Nodes := Uptime} ->
            Uptime;
        _ ->
            Rets = erpc:multicall(Nodes, kmm_utils, erlang_node_uptime, []),
            Uptimes = lists:map(
                        fun
                            ({ok, Uptime}) ->
                        Uptime;
                    (_Error) ->
                        ?kmm_misuse(
                           failed_to_query_erlang_node_uptimes,
                           #{nodes => Nodes,
                             returns => Rets})
                        end, Rets),
            Uptime = lists:max(Uptimes),
            NodeUptimes1 = NodeUptimes#{Nodes => Uptime},
            _ = erlang:put(?ERLANG_NODES_UPTIMES_KEY, NodeUptimes1),
            Uptime
    end.

add_nodes_to_khepri_cluster([Node | Rest], KhepriCluster, StoreId) ->
    case lists:member(Node, KhepriCluster) of
        false ->
            ClusteredNode = hd(KhepriCluster),
            Ret = rpc:call(
                    Node,
                    khepri_cluster, join, [StoreId, ClusteredNode]),
            case Ret of
                ok ->
                    add_nodes_to_khepri_cluster(Rest, KhepriCluster, StoreId);
                Error ->
                    throw(
                      ?kmm_error(
                         failed_to_cluster_khepri_node,
                         #{node => Node,
                           khepri_cluster => KhepriCluster,
                           error => Error}))
            end;
        true ->
            add_nodes_to_khepri_cluster(Rest, KhepriCluster, StoreId)
    end;
add_nodes_to_khepri_cluster([], _KhepriCluster, _StoreId) ->
    ok.

remove_nodes_from_khepri_cluster([Node | Rest], StoreId) ->
    Ret = rpc:call(
            Node,
            khepri_cluster, reset, [StoreId]),
    case Ret of
        ok ->
            remove_nodes_from_khepri_cluster(Rest, StoreId);
        Error ->
            throw(
              ?kmm_error(
                 failed_to_reset_khepri_node,
                 #{node => Node,
                   error => Error}))
    end;
remove_nodes_from_khepri_cluster([], _StoreId) ->
    ok.
