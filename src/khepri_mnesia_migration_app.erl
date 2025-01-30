%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @private

-module(khepri_mnesia_migration_app).
-behaviour(application).

-include_lib("stdlib/include/assert.hrl").

-export([start/2, stop/1]).
-export([enable_repair_cluster_mechanism/1,
         should_repair_cluster/0]).

start(_StartType, _StartArgs) ->
    khepri_mnesia_migration_sup:start_link().

stop(_State) ->
    ok.

enable_repair_cluster_mechanism(Enabled) when is_boolean(Enabled) ->
    application:set_env(
      khepri_mnesia_migration, should_repair_cluster, Enabled,
      [{persistent, true}]).

should_repair_cluster() ->
    ShouldRepairCluster = application:get_env(
                            khepri_mnesia_migration, should_repair_cluster,
                            true),
    ?assertMatch(Value when is_boolean(Value), ShouldRepairCluster),
    ShouldRepairCluster.
