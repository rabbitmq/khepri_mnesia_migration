%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(kmm_gen_servers).

-include_lib("eunit/include/eunit.hrl").

m2k_cluster_sync_test() ->
    Module = m2k_cluster_sync,
    test_gen_server(Module).

m2k_table_copy_test() ->
    Module = m2k_table_copy,
    test_gen_server(Module).

m2k_subscriber_test() ->
    Module = m2k_subscriber,
    test_gen_server(Module).

test_gen_server(Module) ->
    ok = mnesia:start(),
    {atomic, ok} = mnesia:create_table(Module, []),

    RaSystem = Module,
    StoreId = RaSystem,
    StoreDir = helpers:store_dir_name(RaSystem),
    ?assertEqual({ok, StoreId}, khepri:start(StoreDir, StoreId)),

    Args = #{khepri_store => StoreId,
             migration_id => <<"id">>,
             tables => [Module],
             converter_mod => Module},
    {ok, Pid} = Module:start_link(Args),

    ?assertEqual(undefined, gen_server:call(Pid, unknown_call)),
    ?assertEqual(ok, gen_server:cast(Pid, unknown_cast)),
    Pid ! unknown_info,

    ?assert(erlang:is_process_alive(Pid)),

    ?assertEqual(ok, gen_server:stop(Pid)),
    ?assertNot(erlang:is_process_alive(Pid)),

    _ = helpers:remove_store_dir(StoreDir),
    ok.
