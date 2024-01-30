%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @private

-module(m2k_table_copy_sup_sup).

-behaviour(supervisor).

-export([start_link/0,
         prepare_workers_sup/4]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

prepare_workers_sup(StoreId, MigrationId, Tables, Mod) ->
    supervisor:start_child(?SERVER, [#{khepri_store => StoreId,
                                       migration_id => MigrationId,
                                       tables => Tables,
                                       converter_mod => Mod}]).

init([]) ->
    M2KTableCopySup = #{id => m2k_table_copy_sup,
                        restart => temporary,
                        start => {m2k_table_copy_sup, start_link, []}},

    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [M2KTableCopySup],
    {ok, {SupFlags, ChildSpecs}}.
