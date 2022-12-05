-module(m2k_cluster_sync).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_error.hrl").
-include("src/kmm_logging.hrl").

-export([start_link/1,
         proceed/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(?MODULE, {khepri_store}).

proceed(Pid) ->
    case gen_server:call(Pid, ?FUNCTION_NAME) of
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
       [Request]),
    {reply, undefined, State}.

handle_cast(Request, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_cast message: ~p",
       [Request]),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_WARNING(?MODULE_STRING ": Unhandled handle_info message: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

do_sync_cluster(#?MODULE{khepri_store = StoreId}) ->
    ?LOG_INFO(
       "Syncing Mnesia->Khepri clusters membership",
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    MnesiaNodes = kmm_utils:mnesia_nodes(),
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Mnesia cluster: ~0p",
       [MnesiaNodes],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    LargestKhepriCluster = find_largest_khepri_cluster(MnesiaNodes, StoreId),
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Largest Khepri cluster: ~0p",
       [LargestKhepriCluster],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    NodesToAdd = MnesiaNodes -- LargestKhepriCluster,
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Khepri nodes joining the largest "
       "Khepri cluster: ~0p",
       [NodesToAdd],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),

    add_nodes_to_khepri_cluster(NodesToAdd, LargestKhepriCluster, StoreId).

find_largest_khepri_cluster(Nodes, StoreId) ->
    KhepriClusters = list_all_khepri_clusters(Nodes, StoreId),
    KhepriClustersBySize = sort_khepri_clusters_by_size(
                             KhepriClusters, StoreId),
    ?LOG_DEBUG(
       "Mnesia->Khepri cluster sync: Khepri clusters: ~0p",
       [KhepriClustersBySize],
       #{domain => ?KMM_M2K_CLUSTER_SYNC_LOG_DOMAIN}),
    LargestKhepriCluster = hd(KhepriClustersBySize),
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
        AllNodes when is_list(AllNodes) andalso AllNodes =/= [] ->
            SortedNodes = lists:sort(AllNodes),
            SortedNodes;
        [] ->
            ?kmm_misuse(
               khepri_store_must_run,
               #{node => Node,
                 store_id => StoreId});
        Error ->
            throw(
              ?kmm_error(
                 failed_to_query_khepri_nodes,
                 #{node => Node,
                   store_id => StoreId,
                   error => Error}))
    end.

-define(TREE_NODES_COUNTS_KEY, kmm_tree_nodes_counts).

sort_khepri_clusters_by_size(KhepriCluster, StoreId) ->
    _ = erlang:put(?TREE_NODES_COUNTS_KEY, #{}),
    SortedNodes = do_sort_khepri_clusters_by_size(KhepriCluster, StoreId),
    _ = erlang:erase(?TREE_NODES_COUNTS_KEY),
    SortedNodes.

do_sort_khepri_clusters_by_size(KhepriCluster, StoreId) ->
    lists:sort(
      fun(A, B) ->
              ALength = length(A),
              BLength = length(B),
              if
                  ALength =:= BLength ->
                      ATreeNodesCount = get_tree_nodes_count(A, StoreId),
                      BTreeNodesCount = get_tree_nodes_count(B, StoreId),
                      if
                          ATreeNodesCount =:= BTreeNodesCount ->
                              A =< B;
                          true ->
                              ATreeNodesCount > BTreeNodesCount
                      end;
                  true ->
                      length(A) > length(B)
              end
      end,
      KhepriCluster).

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
