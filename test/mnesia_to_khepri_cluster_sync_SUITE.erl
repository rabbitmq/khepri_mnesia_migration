%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates. All rights reserved.
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

         setup_node/1,

         can_create_khepri_cluster_from_mnesia_cluster/1,
         no_data_loss_in_the_largest_khepri_cluster/1,
         no_data_loss_in_the_khepri_cluster_having_data/1,
         mnesia_must_run/1,
         khepri_store_must_run/1]).

all() ->
    [can_create_khepri_cluster_from_mnesia_cluster,
     no_data_loss_in_the_largest_khepri_cluster,
     no_data_loss_in_the_khepri_cluster_having_data,
     mnesia_must_run,
     khepri_store_must_run].

groups() ->
    [].

init_per_suite(Config) ->
    basic_logger_config(),
    ok = cth_log_redirect:handle_remote_events(true),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Testcase, Config) ->
    Nodes = start_n_nodes(Config, Testcase, 5),
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
       rpc:call(SomeNode, mnesia_to_khepri, sync_cluster, [StoreId])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(Nodes, helpers:mnesia_cluster_members(Node)),
              ?assertEqual(
                 Nodes,
                 helpers:khepri_cluster_members(Node, StoreId))
      end, Nodes),

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
       rpc:call(SomeNode, mnesia_to_khepri, sync_cluster, [StoreId])),

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
       rpc:call(SomeNode, mnesia_to_khepri, sync_cluster, [StoreId])),

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
       rpc:call(Node2, mnesia_to_khepri, sync_cluster, [StoreId])),

    ?assertMatch(
       {badrpc,
        {'EXIT',
         {?kmm_exception(
             all_mnesia_nodes_must_run,
             #{all_nodes := Nodes,
               running_nodes := RunningNodes}), _}}},
       rpc:call(Node3, mnesia_to_khepri, sync_cluster, [StoreId])),

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
       rpc:call(Node1, mnesia_to_khepri, sync_cluster, [StoreId])),

    ?assertMatch(
       {badrpc,
        {'EXIT',
         {?kmm_exception(
             khepri_store_must_run,
             #{node := Node2,
               store_id := StoreId}), _}}},
       rpc:call(Node3, mnesia_to_khepri, sync_cluster, [StoreId])),

    ok.

%% -------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

-define(LOGFMT_CONFIG, #{legacy_header => false,
                         single_line => false,
                         template => [time, " ", pid, ": ", msg, "\n"]}).

setup_node(PrivDir) ->
    basic_logger_config(),

    %% We use an additional logger handler for messages tagged with a non-OTP
    %% domain because by default, `cth_log_redirect' drops them.
    GL = erlang:group_leader(),
    GLNode = node(GL),
    Ret = logger:add_handler(
            cth_log_redirect_any_domains, cth_log_redirect_any_domains,
            #{config => #{group_leader => GL,
                          group_leader_node => GLNode}}),
    case Ret of
        ok                          -> ok;
        {error, {already_exist, _}} -> ok
    end,
    ok = logger:set_handler_config(
           cth_log_redirect_any_domains, formatter,
           {logger_formatter, ?LOGFMT_CONFIG}),
    ?LOG_INFO(
       "Extended logger configuration (~s):~n~p",
       [node(), logger:get_config()]),

    Node = node(),
    MnesiaBasename = lists:flatten(
                       io_lib:format("_test.mnesia.~s", [Node])),
    MnesiaDir = filename:join(PrivDir, MnesiaBasename),
    ok = application:set_env(
           mnesia, dir, MnesiaDir, [{persistent, true}]),
    ok = mnesia:create_schema([Node]),

    ok = application:set_env(
           khepri, default_timeout, 5000, [{persistent, true}]),

    ok.

basic_logger_config() ->
    _ = logger:set_primary_config(level, debug),

    HandlerIds = [HandlerId ||
                  HandlerId <- logger:get_handler_ids(),
                  HandlerId =:= default orelse
                  HandlerId =:= cth_log_redirect],
    lists:foreach(
      fun(HandlerId) ->
              ok = logger:set_handler_config(
                    HandlerId, formatter,
                    {logger_formatter, ?LOGFMT_CONFIG}),
              _ = logger:add_handler_filter(
                    HandlerId, progress,
                    {fun logger_filters:progress/2,stop}),
              _ = logger:remove_handler_filter(
                    HandlerId, remote_gl)
      end, HandlerIds),
    ?LOG_INFO(
       "Basic logger configuration (~s):~n~p",
       [node(), logger:get_config()]),

    ok.

start_n_nodes(Config, NamePrefix, Count) ->
    ct:pal("Start ~b Erlang nodes:", [Count]),
    Nodes = [begin
                 Name = lists:flatten(
                          io_lib:format(
                            "~s-~s-~b", [?MODULE, NamePrefix, I])),
                 ct:pal("- ~s", [Name]),
                 start_erlang_node(Name)
             end || I <- lists:seq(1, Count)],
    ct:pal("Started nodes: ~p", [[Node || {Node, _Peer} <- Nodes]]),

    %% We add all nodes to the test coverage report.
    CoveredNodes = [Node || {Node, _Peer} <- Nodes],
    {ok, _} = cover:start([node() | CoveredNodes]),

    CodePath = code:get_path(),
    PrivDir = ?config(priv_dir, Config),
    lists:foreach(
      fun({Node, _Peer}) ->
              rpc:call(Node, code, add_pathsz, [CodePath]),
              ok = rpc:call(Node, ?MODULE, setup_node, [PrivDir])
      end, Nodes),
    Nodes.

-if(?OTP_RELEASE >= 25).
start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    {ok, Peer, Node} = peer:start(#{name => Name1,
                                    wait_boot => infinity}),
    {Node, Peer}.
%stop_erlang_node(_Node, Peer) ->
%    ok = peer:stop(Peer).
-else.
start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    Options = [{monitor_master, true}],
    {ok, Node} = ct_slave:start(Name1, Options),
    {Node, Node}.
%stop_erlang_node(_Node, Node) ->
%    {ok, _} = ct_slave:stop(Node),
%    ok.
-endif.
