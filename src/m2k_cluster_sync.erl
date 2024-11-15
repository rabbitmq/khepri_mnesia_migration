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
-include_lib("khepri/include/khepri.hrl").

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

    NodesToConsider = case MnesiaCluster of
                          [SingleNode] ->
                              %% If the node is unclustered according to
                              %% Mnesia, we consider connected nodes that run
                              %% the Khepri store already and have this node
                              %% among the cluster members they know about.
                              %%
                              %% This allows to repair a cluster where a node
                              %% lost its disk for instance. In ths situation,
                              %% Mnesia thinks it's unclustered. Khepri on
                              %% other nodes will think this lost node is
                              %% already clustered though.
                              %%
                              %% See `find_largest_khepri_cluster/2' for the
                              %% rest of the logic.
                              PossibleNodes = list_possible_nodes(StoreId),
                              ?LOG_DEBUG(
                                 "Mnesia->Khepri cluster sync: "
                                 "Connected nodes to consider: ~0p",
                                 [PossibleNodes],
                                 #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),
                              [SingleNode | PossibleNodes];
                          _ ->
                              MnesiaCluster
                      end,

    LargestKhepriCluster = find_largest_khepri_cluster(
                             NodesToConsider, StoreId),
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
    NodesToRemove = KhepriCluster -- NodesToConsider,
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Khepri nodes being removed from the "
       "expanded Khepri cluster: ~0p",
       [NodesToRemove],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    remove_nodes_from_khepri_cluster(NodesToRemove, StoreId).

list_possible_nodes(StoreId) ->
    %% To detect if this node needs to be added back to an existing cluster, we
    %% check all connected Erlang nodes against the following conditions:
    %%   1. A Khepri store named `StoreId' must be running
    %%   2. That Khepri store must think that this node is part of the cluster
    ConnectedNodes = nodes(),
    ThisMember = khepri_cluster:this_member(StoreId),
    lists:filter(
      fun(Node) ->
              try
                  IsKhepriRunning = erpc:call(
                                      Node, khepri_cluster, is_store_running,
                                      [StoreId]),
                  case IsKhepriRunning of
                      true ->
                          Members = erpc:call(
                                      Node,
                                      khepri_cluster, locally_known_members,
                                      [StoreId]),
                          lists:member(ThisMember, Members);
                      false ->
                          false
                  end
              catch
                  _:_ ->
                      false
              end
      end, ConnectedNodes).

find_largest_khepri_cluster(Nodes, StoreId) ->
    KhepriClusters0 = list_all_khepri_clusters(Nodes, StoreId),
    KhepriClusters1 = remove_khepri_nodes_not_in_mnesia_cluster(
                        Nodes, KhepriClusters0),
    KhepriClusters2 = discard_nodes_who_lost_their_data(KhepriClusters1),
    SortedKhepriClusters = sort_khepri_clusters(KhepriClusters2, StoreId),
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

discard_nodes_who_lost_their_data(KhepriClusters) ->
    discard_nodes_who_lost_their_data(KhepriClusters, KhepriClusters, []).

discard_nodes_who_lost_their_data(
  [[SingleNode] | Rest],
  KhepriClusters,
  LostNodes) ->
    %% We check if a standalore node is also a member of another cluster. It
    %% means the standalore node lost its state and no longer knows that it is
    %% already clustered. Other members consider that it is already clustered
    %% and don't know the node lost its state.
    %%
    %% If we find such a node, we discard it from the list of Khepri clusters
    %% and delete if from the other clusters. This way, the rest of the logic
    %% will consider that the lost node is unclustered.
    IsMemberElsewhere = lists:any(
                          fun
                              (KhepriCluster)
                                when length(KhepriCluster) =:= 1 ->
                                  false;
                              (KhepriCluster) ->
                                  lists:member(SingleNode, KhepriCluster)
                          end, KhepriClusters),
    LostNodes1 = case IsMemberElsewhere of
                     false -> LostNodes;
                     true  -> [SingleNode | LostNodes]
                 end,
    discard_nodes_who_lost_their_data(Rest, KhepriClusters, LostNodes1);
discard_nodes_who_lost_their_data(
  [_KhepriCluster | Rest],
  KhepriClusters,
  LostNodes) ->
    discard_nodes_who_lost_their_data(Rest, KhepriClusters, LostNodes);
discard_nodes_who_lost_their_data([], KhepriClusters, []) ->
    KhepriClusters;
discard_nodes_who_lost_their_data([], KhepriClusters, LostNodes) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: "
       "Nodes who might have lost their data; "
       "they will be considered unclustered: ~0p",
       [lists:sort(LostNodes)],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),
    lists:filtermap(
      fun(KhepriCluster) ->
              KhepriCluster1 = KhepriCluster -- LostNodes,
              case KhepriCluster1 of
                  [] -> false;
                  _  -> {true, KhepriCluster1}
              end
      end, KhepriClusters).

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
    try
        case erpc:call(Node, khepri_cluster, reset, [StoreId]) of
            ok ->
                remove_nodes_from_khepri_cluster(Rest, StoreId);
            {error, ?khepri_error(not_a_khepri_store, _)} ->
                ?LOG_DEBUG(
                   "Mnesia->Khepri cluster sync: Node ~0p does not run the "
                   "Khepri store, skipping its removal from the Khepri "
                   "cluster",
                   [Node],
                   #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN});
            Error ->
                throw(
                  ?kmm_error(
                     failed_to_reset_khepri_node,
                     #{node => Node,
                       error => Error}))
        end
    catch
        error:{erpc, noconnection} ->
            ?LOG_DEBUG(
               "Mnesia->Khepri cluster sync: Node ~0p unreachable, "
               "skipping its removal from the Khepri cluster",
               [Node],
               #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN})
    end;
remove_nodes_from_khepri_cluster([], _StoreId) ->
    ok.
