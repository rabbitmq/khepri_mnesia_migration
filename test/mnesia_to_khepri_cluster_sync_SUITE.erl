%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(mnesia_to_khepri_cluster_sync_SUITE).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("src/kmm_error.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         can_create_khepri_cluster_from_mnesia_cluster/1,
         nodes_not_clustered_in_mnesia_are_removed_from_khepri_1/1,
         nodes_not_clustered_in_mnesia_are_removed_from_khepri_2/1,
         no_data_loss_in_the_largest_khepri_cluster/1,
         no_data_loss_in_the_khepri_cluster_having_data/1,
         can_recreate_khepri_cluster_after_losing_one_node/1,
         can_recreate_khepri_cluster_after_losing_many_nodes_1/1,
         can_recreate_khepri_cluster_after_losing_many_nodes_2/1,
         mnesia_must_run/1,
         khepri_store_must_run/1,
         sort_khepri_clusters_by_members_count/1,
         sort_khepri_clusters_by_tree_nodes_count/1,
         sort_khepri_clusters_by_erlang_node_uptime/1,
         sort_khepri_clusters_by_erlang_node_name/1]).

all() ->
    [can_create_khepri_cluster_from_mnesia_cluster,
     nodes_not_clustered_in_mnesia_are_removed_from_khepri_1,
     nodes_not_clustered_in_mnesia_are_removed_from_khepri_2,
     no_data_loss_in_the_largest_khepri_cluster,
     no_data_loss_in_the_khepri_cluster_having_data,

     %% FIXME: There is still an issue triggered by the following tests where,
     %% on restart, the lost node receives an unexpected `append_entry' RPC
     %% from the cluster and dies with
     %% `leader_saw_append_entries_rpc_in_same_term'.
     can_recreate_khepri_cluster_after_losing_one_node,
     can_recreate_khepri_cluster_after_losing_many_nodes_1,
     can_recreate_khepri_cluster_after_losing_many_nodes_2,

     mnesia_must_run,
     khepri_store_must_run,
     sort_khepri_clusters_by_members_count,
     sort_khepri_clusters_by_tree_nodes_count,
     sort_khepri_clusters_by_erlang_node_uptime,
     sort_khepri_clusters_by_erlang_node_name].

groups() ->
    [].

init_per_suite(Config) ->
    helpers:basic_logger_config(),
    ok = cth_log_redirect:handle_remote_events(true),
    ok = helpers:start_epmd(),
    case net_kernel:start(?MODULE, #{name_domain => shortnames}) of
        {ok, _} ->
            [{started_net_kernel, true} | Config];
        _ ->
            ?assertNotEqual(nonode@nohost, node()),
            [{started_net_kernel, false} | Config]
    end.

end_per_suite(Config) ->
    _ = case ?config(started_net_kernel, Config) of
            true  -> net_kernel:stop();
            false -> ok
        end,
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Testcase, Config) ->
    Nodes = helpers:start_n_nodes(Config, Testcase, 5),
    PropsPerNode0 = [begin
                         ok = rpc:call(Node, mnesia, start, []),
                         {ok, _} = rpc:call(
                                     Node, application, ensure_all_started,
                                     [khepri_mnesia_migration]),
                         Props = rpc:call(
                                   Node, helpers, start_ra_system,
                                   [Testcase]),
                         {Node, Props}
                     end || {Node, _Peer} <- Nodes],
    PropsPerNode = maps:from_list(PropsPerNode0),
    [{ra_system_props, PropsPerNode}, {peer_nodes, Nodes} | Config].

end_per_testcase(_Testcase, Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    maps:fold(
      fun(Node, Props, Acc) ->
              _ = rpc:call(Node, helpers, stop_ra_system, [Props]),
              Acc
      end, ok, PropsPerNode),
    Nodes = ?config(peer_nodes, Config),
    helpers:stop_nodes(Nodes),
    ok.

can_create_khepri_cluster_from_mnesia_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId])),

              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 [Node],
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ok.

nodes_not_clustered_in_mnesia_are_removed_from_khepri_1(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    [Node1, Node2, Node3, Node4, Node5] = Nodes,
    SomeNode = Node4,

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    MnesiaCluster = [Node1, Node2, Node3],
    helpers:cluster_mnesia_nodes(MnesiaCluster),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 MnesiaCluster,
                 helpers:mnesia_cluster_members(Node))
      end, MnesiaCluster),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 [Node],
                 helpers:mnesia_cluster_members(Node))
      end, [Node4, Node5]),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId])),
              ?assertEqual(
                 [Node],
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 ok,
                 rpc:call(Node, khepri_cluster, join, [StoreId, Node1]))
      end, tl(Nodes)),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(
         Node2, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 MnesiaCluster,
                 helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 MnesiaCluster,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, MnesiaCluster),

    ok.

nodes_not_clustered_in_mnesia_are_removed_from_khepri_2(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    [Node1, Node2, Node3, Node4, Node5] = Nodes,
    SomeNode = Node4,

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    MnesiaCluster = [Node1, Node2, Node3],
    helpers:cluster_mnesia_nodes(MnesiaCluster),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 MnesiaCluster,
                 helpers:mnesia_cluster_members(Node))
      end, MnesiaCluster),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 [Node],
                 helpers:mnesia_cluster_members(Node))
      end, [Node4, Node5]),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId])),
              ?assertEqual(
                 [Node],
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(Node4, khepri_cluster, join, [StoreId, Node3])),
    ?assertEqual(
       ok,
       rpc:call(Node5, khepri_cluster, join, [StoreId, Node3])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 [Node3, Node4, Node5],
                 helpers:khepri_cluster_members(Node, StoreId))
      end, [Node3, Node4, Node5]),

    ?assertEqual(
       ok,
       rpc:call(
         Node2, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 MnesiaCluster,
                 helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 MnesiaCluster,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, MnesiaCluster),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 [Node],
                 helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 [Node4, Node5],
                 helpers:khepri_cluster_members(Node, StoreId))
      end, [Node4, Node5]),

    ok.

no_data_loss_in_the_largest_khepri_cluster(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, _, Node3, Node4, _] = Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = Node3,

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(Node3, khepri_cluster, join, [StoreId, Node4])),

    ?assertEqual(
       ok,
       rpc:call(Node1, khepri, put, [StoreId, "/:foo", value_to_throw])),
    ?assertEqual(
       {ok, value_to_throw},
       rpc:call(Node1, khepri, get, [StoreId, "/:foo"])),

    ?assertEqual(
       ok,
       rpc:call(Node3, khepri, put, [StoreId, "/:foo", value_to_keep])),
    ?assertEqual(
       {ok, value_to_keep},
       rpc:call(Node4, khepri, get, [StoreId, "/:foo"])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              case Node =:= Node3 orelse Node =:= Node4 of
                  true ->
                      ?assertEqual(
                         [Node3, Node4],
                         helpers:khepri_cluster_members(Node, StoreId));
                  false ->
                      ?assertEqual(
                         [Node],
                         helpers:khepri_cluster_members(Node, StoreId))
              end
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       {ok, value_to_keep},
       rpc:call(Node1, khepri, get, [StoreId, "/:foo"])),
    ?assertEqual(
       {ok, value_to_keep},
       rpc:call(Node4, khepri, get, [StoreId, "/:foo"])),

    ok.

no_data_loss_in_the_khepri_cluster_having_data(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, _, Node3, Node4, _] = Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = Node3,

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(Node1, khepri, put, [StoreId, "/:foo", value_to_throw])),
    ?assertEqual(
       {ok, value_to_throw},
       rpc:call(Node1, khepri, get, [StoreId, "/:foo"])),

    ?assertEqual(
       ok,
       rpc:call(Node3, khepri, put, [StoreId, "/:bar", value_to_keep])),
    ?assertEqual(
       ok,
       rpc:call(Node3, khepri, put, [StoreId, "/:baz", value_to_keep])),
    ?assertEqual(
       {ok, #{[bar] => value_to_keep,
              [baz] => value_to_keep}},
       rpc:call(Node3, khepri, get_many, [StoreId, "/*"])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 [Node],
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       {error,
        {khepri, node_not_found,
         #{node_name => foo,
           node_path => [foo],
           node_is_target => true}}},
       rpc:call(Node4, khepri, get, [StoreId, "/:foo"])),
    ?assertEqual(
       {ok, #{[bar] => value_to_keep,
              [baz] => value_to_keep}},
       rpc:call(Node4, khepri, get_many, [StoreId, "/*"])),

    ok.

can_recreate_khepri_cluster_after_losing_one_node(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 erpc:call(Node, khepri, start, [RaSystem, StoreId])),

              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 [Node],
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    Path = [foo],
    Value = bar,
    ?assertEqual(ok, erpc:call(SomeNode, khepri, put, [StoreId, Path, Value])),

    %% Add a random node to the mix.
    {RandomNode, RandomPeer} = helpers:start_erlang_node("random-node"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(pong, erpc:call(RandomNode, net_adm, ping, [Node]))
      end, Nodes),

    %% Stop Khepri store on node 1 and reset it.
    ct:pal("Stopping and \"losing\" a node"),
    [LostNode | _] = Nodes,
    LostNodeProps = maps:get(LostNode, PropsPerNode),
    erpc:call(LostNode, mnesia, stop, []),
    erpc:call(LostNode, khepri, stop, [StoreId]),
    LostNodeProps1 = erpc:call(
                       LostNode, helpers, reset_ra_system, [LostNodeProps]),

    ct:pal("Resetting Mnesia on lost node"),
    MnesiaDir = erpc:call(LostNode, mnesia, system_info, [directory]),
    ok = helpers:remove_store_dir(MnesiaDir),
    erpc:call(LostNode, mnesia, start, []),

    ct:pal("Wait for leader on remaining nodes"),
    ok = erpc:call(
           SomeNode, khepri_cluster, wait_for_leader, [StoreId, infinity]),

    ct:pal("Restarting Khepri on lost node"),
    ?assertEqual(
       {ok, StoreId},
       erpc:call(LostNode, khepri, start, [RaSystem, StoreId])),

    ?assertEqual(
       lists:sort([node(), RandomNode | Nodes] -- [LostNode]),
       lists:sort(erpc:call(LostNode, erlang, nodes, []))),

    ct:pal("Sync membership on lost node"),
    ?assertEqual(
       ok,
       erpc:call(
         LostNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    ct:pal("Verifying cluster"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       {ok, Value},
       erpc:call(LostNode, khepri, get, [StoreId, Path])),

    _ = erpc:call(LostNode, helpers, stop_ra_system, [LostNodeProps1]),

    helpers:stop_erlang_node(RandomNode, RandomPeer),

    ok.

can_recreate_khepri_cluster_after_losing_many_nodes_1(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:last(Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 erpc:call(Node, khepri, start, [RaSystem, StoreId])),

              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 [Node],
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    Path = [foo],
    Value = bar,
    ?assertEqual(ok, erpc:call(SomeNode, khepri, put, [StoreId, Path, Value])),

    %% Add a random node to the mix.
    {RandomNode, RandomPeer} = helpers:start_erlang_node("random-node"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(pong, erpc:call(RandomNode, net_adm, ping, [Node]))
      end, Nodes),

    %% Stop Khepri store on node 1 and node 2, and reset it.
    ct:pal("Stopping and \"losing\" node 1 and node 2"),
    [LostNode1, LostNode2 | _] = Nodes,
    LostNode1Props = maps:get(LostNode1, PropsPerNode),
    erpc:call(LostNode1, mnesia, stop, []),
    erpc:call(LostNode1, khepri, stop, [StoreId]),
    LostNode1Props1 = erpc:call(
                        LostNode1, helpers, reset_ra_system, [LostNode1Props]),
    LostNode2Props = maps:get(LostNode2, PropsPerNode),
    erpc:call(LostNode2, mnesia, stop, []),
    erpc:call(LostNode2, khepri, stop, [StoreId]),
    LostNode2Props1 = erpc:call(
                        LostNode2, helpers, reset_ra_system, [LostNode2Props]),

    ct:pal("Resetting Mnesia on lost node 1 and node 2"),
    MnesiaDir1 = erpc:call(LostNode1, mnesia, system_info, [directory]),
    ok = helpers:remove_store_dir(MnesiaDir1),
    erpc:call(LostNode1, mnesia, start, []),
    MnesiaDir2 = erpc:call(LostNode2, mnesia, system_info, [directory]),
    ok = helpers:remove_store_dir(MnesiaDir2),
    erpc:call(LostNode2, mnesia, start, []),

    ct:pal("Wait for leader on remaining nodes"),
    ok = erpc:call(
           SomeNode, khepri_cluster, wait_for_leader, [StoreId, infinity]),

    ct:pal("Restarting Khepri on lost node 1"),
    ?assertEqual(
       {ok, StoreId},
       erpc:call(LostNode1, khepri, start, [RaSystem, StoreId])),

    ?assertEqual(
       lists:sort([node(), RandomNode | Nodes] -- [LostNode1]),
       lists:sort(erpc:call(LostNode1, erlang, nodes, []))),

    ct:pal("Sync membership on lost node"),
    ?assertEqual(
       ok,
       erpc:call(
         LostNode1, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    ct:pal("Verifying cluster"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes -- [LostNode2]),

    ?assertEqual(
       {ok, Value},
       erpc:call(LostNode1, khepri, get, [StoreId, Path])),

    _ = erpc:call(LostNode1, helpers, stop_ra_system, [LostNode1Props1]),
    _ = erpc:call(LostNode2, helpers, stop_ra_system, [LostNode2Props1]),

    helpers:stop_erlang_node(RandomNode, RandomPeer),

    ok.

can_recreate_khepri_cluster_after_losing_many_nodes_2(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:last(Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 erpc:call(Node, khepri, start, [RaSystem, StoreId])),

              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 [Node],
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

    Path = [foo],
    Value = bar,
    ?assertEqual(ok, erpc:call(SomeNode, khepri, put, [StoreId, Path, Value])),

    %% Add a random node to the mix.
    {RandomNode, RandomPeer} = helpers:start_erlang_node("random-node"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(pong, erpc:call(RandomNode, net_adm, ping, [Node]))
      end, Nodes),

    %% Stop Khepri store on node 1 and node 2, and reset it.
    ct:pal("Stopping and \"losing\" node 1 and node 2"),
    [LostNode1, LostNode2 | _] = Nodes,
    LostNode1Props = maps:get(LostNode1, PropsPerNode),
    erpc:call(LostNode1, mnesia, stop, []),
    erpc:call(LostNode1, khepri, stop, [StoreId]),
    LostNode1Props1 = erpc:call(
                        LostNode1, helpers, reset_ra_system, [LostNode1Props]),

    ct:pal("Resetting Mnesia on lost node 1"),
    MnesiaDir1 = erpc:call(LostNode1, mnesia, system_info, [directory]),
    ok = helpers:remove_store_dir(MnesiaDir1),
    erpc:call(LostNode1, mnesia, start, []),

    ct:pal("Stopping node 2 Erlang VM"),
    NodePeers = ?config(peer_nodes, Config),
    LostNode2Peer = proplists:get_value(LostNode2, NodePeers),
    helpers:stop_erlang_node(LostNode2, LostNode2Peer),

    ct:pal("Wait for leader on remaining nodes"),
    ok = erpc:call(
           SomeNode, khepri_cluster, wait_for_leader, [StoreId, infinity]),

    ct:pal("Restarting Khepri on lost node 1"),
    ?assertEqual(
       {ok, StoreId},
       erpc:call(LostNode1, khepri, start, [RaSystem, StoreId])),

    ?assertEqual(
       lists:sort([node(), RandomNode | Nodes] -- [LostNode1, LostNode2]),
       lists:sort(erpc:call(LostNode1, erlang, nodes, []))),

    ct:pal("Sync membership on lost node"),
    ?assertEqual(
       ok,
       erpc:call(
         LostNode1, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    ct:pal("Verifying cluster"),
    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes -- [LostNode2]),

    ?assertEqual(
       {ok, Value},
       erpc:call(LostNode1, khepri, get, [StoreId, Path])),

    _ = erpc:call(LostNode1, helpers, stop_ra_system, [LostNode1Props1]),

    helpers:stop_erlang_node(RandomNode, RandomPeer),

    ok.

mnesia_must_run(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3, _, _] = Nodes = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    helpers:cluster_mnesia_nodes(Nodes),

    ?assertEqual(
       stopped,
       rpc:call(Node2, mnesia, stop, [])),
    RunningNodes = Nodes -- [Node2],

    ?assertMatch(
       {badrpc,
        {'EXIT',
         {?kmm_exception(
             mnesia_must_run,
             #{node := Node2}), _}}},
       rpc:call(Node2, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    lists:foreach(
      fun(_) ->
              Ret = rpc:call(
                      Node3,
                      mnesia_to_khepri, sync_cluster_membership, [StoreId]),
              case Ret of
                  {badrpc,
                   {'EXIT',
                    {?kmm_exception(all_mnesia_nodes_must_run, _)}}} ->
                      ok;
                  _ ->
                      timer:sleep(500)
              end
      end, lists:seq(1, 20)),

    ?assertMatch(
       {badrpc,
        {'EXIT',
         {?kmm_exception(
             all_mnesia_nodes_must_run,
             #{all_nodes := Nodes,
               running_nodes := RunningNodes}), _}}},
       rpc:call(Node3, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    ok.

khepri_store_must_run(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1, Node2, Node3, _, _] = Nodes = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    helpers:cluster_mnesia_nodes(Nodes),

    ?assertEqual(
       {ok, StoreId},
       rpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    ?assertMatch(
       {badrpc,
        {'EXIT',
         {?kmm_exception(
             khepri_store_must_run,
             #{node := Node2,
               store_id := StoreId}), _}}},
       rpc:call(Node1, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    ?assertMatch(
       {badrpc,
        {'EXIT',
         {?kmm_exception(
             khepri_store_must_run,
             #{node := Node2,
               store_id := StoreId}), _}}},
       rpc:call(Node3, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    ok.

sort_khepri_clusters_by_members_count(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    [Node1, Node2, Node3, Node4, Node5] = Nodes,
    SomeNode = Node5,

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(Node3, khepri_cluster, join, [StoreId, Node4])),

    KhepriClusters = randomize_list([[Node1],
                                     [Node2],
                                     [Node3, Node4],
                                     [Node5]]),
    SortedKhepriClusters = [[Node3, Node4],
                            [Node1],
                            [Node2],
                            [Node5]],
    ?assertEqual(
       SortedKhepriClusters,
       m2k_cluster_sync:sort_khepri_clusters(KhepriClusters, StoreId)),

    ok.

sort_khepri_clusters_by_tree_nodes_count(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    [Node1, Node2, Node3, Node4, Node5] = Nodes,
    SomeNode = Node5,

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(Node4, khepri, put, [StoreId, "/:foo", value])),
    ?assertEqual(
       {ok, #{[foo] => value}},
       rpc:call(Node4, khepri, get_many, [StoreId, "/*"])),

    KhepriClusters = randomize_list([[Node1],
                                     [Node2],
                                     [Node3],
                                     [Node4],
                                     [Node5]]),
    SortedKhepriClusters = [[Node4],
                            [Node1],
                            [Node2],
                            [Node3],
                            [Node5]],
    ?assertEqual(
       SortedKhepriClusters,
       m2k_cluster_sync:sort_khepri_clusters(KhepriClusters, StoreId)),

    ok.

sort_khepri_clusters_by_erlang_node_uptime(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    [Node1, Node2, Node3, Node4, Node5] = Nodes,
    SomeNode = Node5,

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    KhepriClusters = randomize_list([[Node1],
                                     [Node2],
                                     [Node3],
                                     [Node4],
                                     [Node5]]),
    SortedKhepriClusters = [[Node1],
                            [Node2],
                            [Node3],
                            [Node4],
                            [Node5]],
    ?assertEqual(
       SortedKhepriClusters,
       m2k_cluster_sync:sort_khepri_clusters(KhepriClusters, StoreId)),

    ok.

sort_khepri_clusters_by_erlang_node_name(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,

    ?assertEqual(
       {ok, StoreId},
       rpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    ?assertEqual(
       [[Node1], [Node1]],
       m2k_cluster_sync:sort_khepri_clusters([[Node1], [Node1]], StoreId)),

    ok.

randomize_list(List) ->
    [X
     || {_, X} <- lists:sort([{rand:uniform(), N} || N <- List])].
