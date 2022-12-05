%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(mnesia_to_khepri_data_copy_SUITE).

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

         can_copy_existing_data_from_mnesia_to_khepri/1]).

all() ->
    [can_copy_existing_data_from_mnesia_to_khepri].

groups() ->
    [].

init_per_suite(Config) ->
    helpers:basic_logger_config(),
    ok = cth_log_redirect:handle_remote_events(true),
    Config.

end_per_suite(_Config) ->
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
    ok.

can_copy_existing_data_from_mnesia_to_khepri(Config) ->
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
                 rpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    ?assertEqual(
       ok,
       rpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    ?assertEqual(
       ok,
       rpc:call(SomeNode, mnesia_to_khepri, copy_data, [StoreId])),

    ok.
