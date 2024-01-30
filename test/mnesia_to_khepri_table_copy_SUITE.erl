%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(mnesia_to_khepri_table_copy_SUITE).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("khepri/include/khepri.hrl").

-include("src/kmm_error.hrl").

-export([all/0,
         suite/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         can_copy_existing_data_from_mnesia_to_khepri/1,
         can_copy_data_added_concurrently_from_mnesia_to_khepri/1,
         can_handle_non_existing_tables/1,
         only_one_migration_allowed_at_a_time/1,
         converter_mod_crashing_during_init/1,
         converter_mod_crashing_during_copy/1,
         converter_mod_crashing_during_finish/1,
         is_migration_finished_works/1,
         wait_for_migration_works/1,
         can_cleanup_after_successful_migration/1,
         refuse_to_cleanup_during_migration/1,
         cannot_cleanup_a_non_existing_migration/1,
         can_rollback_after_successful_migration/1,
         refuse_to_rollback_during_migration/1,
         cannot_rollback_a_non_existing_migration/1,
         handle_fallback_with_mnesia_dirty_op_works/1,
         handle_fallback_with_mnesia_tx_works/1,
         handle_fallback_with_khepri_ret/1]).

-record(my_record, {key, value}).

all() ->
    [can_copy_existing_data_from_mnesia_to_khepri,
     can_copy_data_added_concurrently_from_mnesia_to_khepri,
     can_handle_non_existing_tables,
     only_one_migration_allowed_at_a_time,
     converter_mod_crashing_during_init,
     converter_mod_crashing_during_copy,
     converter_mod_crashing_during_finish,
     is_migration_finished_works,
     wait_for_migration_works,
     can_cleanup_after_successful_migration,
     refuse_to_cleanup_during_migration,
     cannot_cleanup_a_non_existing_migration,
     can_rollback_after_successful_migration,
     refuse_to_rollback_during_migration,
     cannot_rollback_a_non_existing_migration,
     handle_fallback_with_mnesia_dirty_op_works,
     handle_fallback_with_mnesia_tx_works,
     handle_fallback_with_khepri_ret].

suite() ->
    [{timetrap, {minutes, 5}}].

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

can_copy_existing_data_from_mnesia_to_khepri(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

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
       {atomic, ok},
       rpc:call(
         SomeNode, mnesia, create_table,
         [my_table1, [{record_name, my_record},
                      {attributes, record_info(fields, my_record)},
                      {disc_copies, Nodes}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         SomeNode, mnesia, create_table,
         [my_table2, [{record_name, my_record},
                      {attributes, record_info(fields, my_record)},
                      {disc_copies, Nodes}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         SomeNode, mnesia, transaction,
         [fun() ->
                  lists:foreach(
                    fun(I) ->
                            Key = make_key(I),
                            Value = make_value(I),
                            Record = #my_record{key = Key, value = Value},
                            mnesia:write(my_table1, Record, write),
                            mnesia:write(my_table2, Record, write)
                    end, lists:seq(1, 1000))
          end])),
    MnesiaObjects1 = rpc:call(
                       SomeNode,
                       mnesia, dirty_match_object,
                       [my_table1, #my_record{_ = '_'}]),
    MnesiaObjects2 = rpc:call(
                       SomeNode,
                       mnesia, dirty_match_object,
                       [my_table2, #my_record{_ = '_'}]),

    ?assertEqual(
       ok,
       rpc:call(
         SomeNode, mnesia_to_khepri, copy_all_tables,
         [StoreId, MigrationId, mnesia_to_khepri_example_converter])),

    {ok, KhepriObjects1} = rpc:call(
                             SomeNode,
                             khepri, get_many,
                             [StoreId, [my_table1, ?KHEPRI_WILDCARD_STAR]]),
    ?assertNotEqual([], MnesiaObjects1),
    ?assertEqual(
       lists:sort(MnesiaObjects1),
       lists:sort(maps:values(KhepriObjects1))),

    {ok, KhepriObjects2} = rpc:call(
                             SomeNode,
                             khepri, get_many,
                             [StoreId, [my_table2, ?KHEPRI_WILDCARD_STAR]]),
    ?assertNotEqual([], MnesiaObjects2),
    ?assertEqual(
       lists:sort(MnesiaObjects2),
       lists:sort(maps:values(KhepriObjects2))),

    ok.

can_copy_data_added_concurrently_from_mnesia_to_khepri(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

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
       {atomic, ok},
       rpc:call(
         SomeNode, mnesia, create_table,
         [my_record, [{attributes, record_info(fields, my_record)},
                      {disc_copies, Nodes}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         SomeNode, mnesia, transaction,
         [fun() ->
                  lists:foreach(
                    fun(I) ->
                            Key = make_key(I),
                            Value = make_value(I),
                            mnesia:write(#my_record{key = Key, value = Value})
                    end, lists:seq(1, 1000))
          end])),

    Parent = self(),
    Count = 1000,
    Worker =
    spawn_link(
      fun() ->
              Pid = self(),
              lists:foreach(
                fun(I) ->
                        Key = make_key(I),
                        Value = make_value(I),
                        Ret1 = rpc:call(
                                 SomeNode, mnesia, transaction,
                                 [fun() ->
                                          Record = #my_record{key = Key,
                                                              value = Value},
                                          mnesia:write(Record),
                                          Parent ! {Pid, write, Record}
                                  end]),
                        log_failed_tx(Ret1),

                        if
                            I rem 5 =:= 0 ->
                                PreviousKey = make_key(I - 1),
                                Ret2 = rpc:call(
                                         SomeNode, mnesia, transaction,
                                         [fun() ->
                                                  mnesia:delete(
                                                    my_record,
                                                    PreviousKey,
                                                    write),
                                                  Parent ! {Pid,
                                                            delete,
                                                            PreviousKey}
                                          end]),
                                log_failed_tx(Ret2);
                            true ->
                                ok
                        end
                end, lists:seq(1, Count)),
              Parent ! {Pid, done}
      end),
    timer:sleep(100),

    ?assertEqual(
       ok,
       rpc:call(
         SomeNode, mnesia_to_khepri, copy_all_tables,
         [StoreId, MigrationId, mnesia_to_khepri_example_converter])),

    MnesiaObjects =
    lists:foldl(
      fun
          (_I, {acc, MO} = Acc) ->
              receive
                  {Worker, write, Record} ->
                      MO1 = [Record | MO],
                      {acc, MO1};
                  {Worker, delete, Key} ->
                      MO1 = lists:keydelete(Key, #my_record.key, MO),
                      {acc, MO1};
                  {Worker, done} ->
                      MO
              after 100 ->
                        Acc
              end;
          (_I, MO) ->
              MO
      end, {acc, []}, lists:seq(1, erlang:round(Count * 1.3))),

    {ok, KhepriObjects} = rpc:call(
                            SomeNode,
                            khepri, get_many,
                            [StoreId, [my_record, ?KHEPRI_WILDCARD_STAR]]),

    ?assertNotEqual([], MnesiaObjects),
    ?assertEqual(
       lists:sort(MnesiaObjects),
       lists:sort(maps:values(KhepriObjects))),

    ok.

can_handle_non_existing_tables(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

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
       {atomic, ok},
       rpc:call(
         SomeNode, mnesia, create_table,
         [my_table1, [{record_name, my_record},
                      {attributes, record_info(fields, my_record)},
                      {disc_copies, Nodes}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         SomeNode, mnesia, transaction,
         [fun() ->
                  lists:foreach(
                    fun(I) ->
                            Key = make_key(I),
                            Value = make_value(I),
                            Record = #my_record{key = Key, value = Value},
                            mnesia:write(my_table1, Record, write)
                    end, lists:seq(1, 1000))
          end])),
    MnesiaObjects1 = rpc:call(
                       SomeNode,
                       mnesia, dirty_match_object,
                       [my_table1, #my_record{_ = '_'}]),

    ?assertEqual(
       ok,
       rpc:call(
         SomeNode, mnesia_to_khepri, copy_tables,
         [StoreId, MigrationId, [my_table1, my_non_existing_table2],
          mnesia_to_khepri_example_converter])),

    {ok, KhepriObjects1} = rpc:call(
                             SomeNode,
                             khepri, get_many,
                             [StoreId, [my_table1, ?KHEPRI_WILDCARD_STAR]]),
    ?assertNotEqual([], MnesiaObjects1),
    ?assertEqual(
       lists:sort(MnesiaObjects1),
       lists:sort(maps:values(KhepriObjects1))),

    ok.

only_one_migration_allowed_at_a_time(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

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

    Table = my_record,
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         SomeNode, mnesia, create_table,
         [Table, [{attributes, record_info(fields, my_record)},
                  {disc_copies, Nodes}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         SomeNode, mnesia, transaction,
         [fun() ->
                  lists:foreach(
                    fun(I) ->
                            Key = make_key(I),
                            Value = make_value(I),
                            mnesia:write(#my_record{key = Key, value = Value})
                    end, lists:seq(1, 1000))
          end])),
    MnesiaObjects = rpc:call(
                      SomeNode,
                      mnesia, dirty_match_object, [#my_record{_ = '_'}]),

    Parent = self(),

    FakeMigration = spawn_link(
                      fun() ->
                              Path = m2k_table_copy:marker_path(MigrationId),
                              Progress =
                              m2k_table_copy:migration_recorded_state(
                                self(), []),
                              ok = rpc:call(
                                     SomeNode,
                                     khepri, create,
                                     [StoreId, Path, Progress]),
                              Parent ! fake_migration_ready,
                              receive
                                  stop ->
                                      unlink(Parent),
                                      %% We don't delete the `in_flight' marker
                                      %% to simulate a crashed migration. The
                                      %% next attempt should delete the marker
                                      %% because it belongs to a non-existing
                                      %% process.
                                      ok
                              end
                      end),

    receive fake_migration_ready -> ok end,

    Pids = [spawn(
              fun() ->
                      Ret = rpc:call(
                              SomeNode, mnesia_to_khepri, copy_all_tables,
                              [StoreId, MigrationId, mnesia_to_khepri_example_converter]),
                      Parent ! {self(), Ret}
              end) || _ <- lists:seq(1, 5)],

    %% Terminate the fake "migration" that did nothing.
    timer:sleep(2000),
    FakeMigration ! stop,

    Rets = lists:map(
             fun(Pid) ->
                     receive {Pid, Ret} -> Ret end
             end, Pids),
    ?assertEqual(Rets, [ok || ok <- Rets]),

    {ok, KhepriObjects} = rpc:call(
                            SomeNode,
                            khepri, get_many,
                            [StoreId, [my_record, ?KHEPRI_WILDCARD_STAR]]),

    ?assertNotEqual([], MnesiaObjects),
    ?assertEqual(
       lists:sort(MnesiaObjects),
       lists:sort(maps:values(KhepriObjects))),

    ok.

make_key(I) ->
    list_to_binary(io_lib:format("key_~b", [I])).

make_value(I) ->
    io_lib:format("value_~b", [I]).

log_failed_tx({atomic, _}) ->
    ok;
log_failed_tx({aborted, _} = Aborted) ->
    ct:pal("Failed tx: ~0p", [Aborted]).

converter_mod_crashing_during_init(Config) ->
    StoreId = crash_during_init,
    converter_mod_crashing_during_something(Config, StoreId).

converter_mod_crashing_during_copy(Config) ->
    StoreId = crash_during_copy,
    converter_mod_crashing_during_something(Config, StoreId).

converter_mod_crashing_during_finish(Config) ->
    StoreId = crash_during_finish,
    converter_mod_crashing_during_something(Config, StoreId).

converter_mod_crashing_during_something(Config, StoreId) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    MigrationId = store_id_to_mig_id(StoreId),

    ?assertEqual(
       {ok, StoreId},
       rpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    Table = my_record,
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, create_table,
         [Table, [{attributes, record_info(fields, my_record)},
                  {disc_copies, [Node1]}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, transaction,
         [fun() ->
                  I = 1,
                  Key = make_key(I),
                  Value = make_value(I),
                  mnesia:write(#my_record{key = Key, value = Value})
          end])),

    Mod = crashing_converter,
    ?assertError(
       {exception,
        ?kmm_exception(
           converter_mod_exception,
           #{converter_mod := Mod,
             reason := {crash, StoreId}}),
        _},
       erpc:call(
         Node1, mnesia_to_khepri, copy_all_tables,
         [StoreId, MigrationId, Mod])),

    %% If the conversion crashes, the table should not be marked as migrated.
    ?assertEqual(
       false,
       erpc:call(
         Node1,
         mnesia_to_khepri, is_migration_finished, [StoreId, MigrationId])),

    ?assertEqual(
       read_write,
       rpc:call(Node1, mnesia, table_info, [Table, access_mode])),

    ok.

is_migration_finished_works(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    %% `is_migration_finished/2' works even if the given store is unavailable.
    ?assertEqual(
       false,
       erpc:call(
         Node1,
         mnesia_to_khepri, is_migration_finished, [StoreId, MigrationId])),

    ?assertEqual(
       {ok, StoreId},
       rpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    Table = my_record,
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, create_table,
         [Table, [{attributes, record_info(fields, my_record)},
                  {disc_copies, [Node1]}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, transaction,
         [fun() ->
                  I = 1,
                  Key = make_key(I),
                  Value = make_value(I),
                  mnesia:write(#my_record{key = Key, value = Value})
          end])),

    ?assertEqual(
       false,
       erpc:call(
         Node1,
         mnesia_to_khepri, is_migration_finished, [StoreId, MigrationId])),

    ?assertEqual(
       ok,
       erpc:call(
         Node1, mnesia_to_khepri, copy_all_tables,
         [StoreId, MigrationId, mnesia_to_khepri_example_converter])),

    ?assertEqual(
       true,
       erpc:call(
         Node1,
         mnesia_to_khepri, is_migration_finished, [StoreId, MigrationId])),

    ok.

wait_for_migration_works(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    ?assertEqual(
       {ok, StoreId},
       rpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    Table = my_record,
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, create_table,
         [Table, [{attributes, record_info(fields, my_record)},
                  {disc_copies, [Node1]}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, transaction,
         [fun() ->
                  I = 1,
                  Key = make_key(I),
                  Value = make_value(I),
                  mnesia:write(#my_record{key = Key, value = Value})
          end])),

    Parent = self(),

    ?assertEqual(
       false,
       erpc:call(
         Node1,
         mnesia_to_khepri, wait_for_migration,
         [StoreId, MigrationId, infinity])),

    _ = spawn_link(fun() ->
                           ?assertEqual(
                              ok,
                              erpc:call(
                                Node1, mnesia_to_khepri, copy_all_tables,
                                [StoreId, MigrationId,
                                 {controllable_converter, Parent}])),
                           erlang:unlink(Parent)
                   end),

    receive
        {init_copy_to_khepri, StoreId, Pid} ->
            _ = spawn_link(fun() ->
                                   Ret = erpc:call(
                                           Node1,
                                           mnesia_to_khepri,
                                           wait_for_migration,
                                           [StoreId, MigrationId, 1000]),
                                   Parent ! {?FUNCTION_NAME, Ret},
                                   erlang:unlink(Parent)
                           end),

            ?assertEqual(
               timeout,
               receive
                   {?FUNCTION_NAME, Ret} -> Ret
               end),

            _ = spawn_link(fun() ->
                                   ct:pal("Waiting for migration to finish"),
                                   Ret = erpc:call(
                                           Node1,
                                           mnesia_to_khepri,
                                           wait_for_migration,
                                           [StoreId, MigrationId, infinity]),
                                   Parent ! {?FUNCTION_NAME, Ret},
                                   erlang:unlink(Parent)
                           end),

            ?assertEqual(
               timeout_from_testcase,
               receive
                   {?FUNCTION_NAME, Ret} -> Ret
               after 1000 ->
                         timeout_from_testcase
               end),

            Pid ! proceed
    end,

    ?assertEqual(
       true,
       receive
           {?FUNCTION_NAME, Ret} -> Ret
       after 5000 ->
                 timeout
       end),

    ok.

can_cleanup_after_successful_migration(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 erpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    Tables = [my_table1, my_table2],
    lists:foreach(
      fun(Table) ->
              ?assertEqual(
                 {atomic, ok},
                 erpc:call(
                   SomeNode, mnesia, create_table,
                   [Table, [{record_name, my_record},
                            {attributes, record_info(fields, my_record)},
                            {disc_copies, Nodes}]]))
      end, Tables),

    ?assertEqual(
       {atomic, ok},
       erpc:call(
         SomeNode, mnesia, transaction,
         [fun() ->
                  lists:foreach(
                    fun(I) ->
                            Key = make_key(I),
                            Value = make_value(I),
                            Record = #my_record{key = Key, value = Value},
                            lists:foreach(
                              fun(Table) ->
                                      mnesia:write(Table, Record, write)
                              end, Tables)
                    end, lists:seq(1, 1000))
          end])),

    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, copy_all_tables,
         [StoreId, MigrationId, mnesia_to_khepri_example_converter])),

    ?assertEqual(
       [read_only || _ <- Tables],
       [catch erpc:call(SomeNode, mnesia, table_info, [Table, access_mode])
        || Table <- Tables]),
    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, cleanup_after_table_copy,
         [StoreId, MigrationId])),
    ?assertEqual(
       [{'EXIT', {exception, {aborted,{no_exists, Table, access_mode}}}}
        || Table <- Tables],
       [catch erpc:call(SomeNode, mnesia, table_info, [Table, access_mode])
        || Table <- Tables]),

    ok.

refuse_to_cleanup_during_migration(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    ?assertEqual(
       {ok, StoreId},
       rpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    Table = my_record,
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, create_table,
         [Table, [{attributes, record_info(fields, my_record)},
                  {disc_copies, [Node1]}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, transaction,
         [fun() ->
                  I = 1,
                  Key = make_key(I),
                  Value = make_value(I),
                  mnesia:write(#my_record{key = Key, value = Value})
          end])),

    Parent = self(),

    ?assertEqual(
       false,
       erpc:call(
         Node1,
         mnesia_to_khepri, wait_for_migration,
         [StoreId, MigrationId, infinity])),

    _ = spawn_link(fun() ->
                           ?assertEqual(
                              ok,
                              erpc:call(
                                Node1, mnesia_to_khepri, copy_all_tables,
                                [StoreId, MigrationId,
                                 {controllable_converter, Parent}])),
                           erlang:unlink(Parent)
                   end),

    receive
        {init_copy_to_khepri, StoreId, Pid} ->
            _ = spawn_link(fun() ->
                                   Ret = erpc:call(
                                           Node1,
                                           mnesia_to_khepri,
                                           wait_for_migration,
                                           [StoreId, MigrationId, 1000]),
                                   Parent ! {?FUNCTION_NAME, Ret},
                                   erlang:unlink(Parent)
                           end),

            ?assertEqual(
               timeout,
               receive
                   {?FUNCTION_NAME, Ret} -> Ret
               end),

            ?assertEqual(
               set,
               catch erpc:call(Node1, mnesia, table_info, [Table, type])),
            ?assertEqual(
               {error, {in_flight, Pid}},
               erpc:call(
                 Node1, mnesia_to_khepri, cleanup_after_table_copy,
                 [StoreId, MigrationId])),
            ?assertEqual(
               set,
               catch erpc:call(Node1, mnesia, table_info, [Table, type])),

            _ = spawn_link(fun() ->
                                   ct:pal("Waiting for migration to finish"),
                                   Ret = erpc:call(
                                           Node1,
                                           mnesia_to_khepri,
                                           wait_for_migration,
                                           [StoreId, MigrationId, infinity]),
                                   Parent ! {?FUNCTION_NAME, Ret},
                                   erlang:unlink(Parent)
                           end),

            ?assertEqual(
               timeout_from_testcase,
               receive
                   {?FUNCTION_NAME, Ret} -> Ret
               after 1000 ->
                         timeout_from_testcase
               end),

            Pid ! proceed
    end,

    ?assertEqual(
       true,
       receive
           {?FUNCTION_NAME, Ret} -> Ret
       after 5000 ->
                 timeout
       end),

    ok.

cannot_cleanup_a_non_existing_migration(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 erpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    Tables = [my_table1, my_table2],
    lists:foreach(
      fun(Table) ->
              ?assertEqual(
                 {atomic, ok},
                 erpc:call(
                   SomeNode, mnesia, create_table,
                   [Table, [{record_name, my_record},
                            {attributes, record_info(fields, my_record)},
                            {disc_copies, Nodes}]]))
      end, Tables),

    ?assertEqual(
       {atomic, ok},
       erpc:call(
         SomeNode, mnesia, transaction,
         [fun() ->
                  lists:foreach(
                    fun(I) ->
                            Key = make_key(I),
                            Value = make_value(I),
                            Record = #my_record{key = Key, value = Value},
                            lists:foreach(
                              fun(Table) ->
                                      mnesia:write(Table, Record, write)
                              end, Tables)
                    end, lists:seq(1, 1000))
          end])),

    ?assertEqual(
       [read_write || _ <- Tables],
       [catch erpc:call(SomeNode, mnesia, table_info, [Table, access_mode])
        || Table <- Tables]),
    ?assertEqual(
       {error, {no_such_migration, MigrationId}},
       erpc:call(
         SomeNode, mnesia_to_khepri, cleanup_after_table_copy,
         [StoreId, MigrationId])),
    ?assertEqual(
       [read_write || _ <- Tables],
       [catch erpc:call(SomeNode, mnesia, table_info, [Table, access_mode])
        || Table <- Tables]),

    ok.

can_rollback_after_successful_migration(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 erpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    Tables = [my_table1, my_table2],
    lists:foreach(
      fun(Table) ->
              ?assertEqual(
                 {atomic, ok},
                 erpc:call(
                   SomeNode, mnesia, create_table,
                   [Table, [{record_name, my_record},
                            {attributes, record_info(fields, my_record)},
                            {disc_copies, Nodes}]]))
      end, Tables),

    ?assertEqual(
       {atomic, ok},
       erpc:call(
         SomeNode, mnesia, transaction,
         [fun() ->
                  lists:foreach(
                    fun(I) ->
                            Key = make_key(I),
                            Value = make_value(I),
                            Record = #my_record{key = Key, value = Value},
                            lists:foreach(
                              fun(Table) ->
                                      mnesia:write(Table, Record, write)
                              end, Tables)
                    end, lists:seq(1, 1000))
          end])),

    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, copy_all_tables,
         [StoreId, MigrationId, mnesia_to_khepri_example_converter])),

    ?assertEqual(
       true,
       erpc:call(
         SomeNode,
         mnesia_to_khepri, is_migration_finished, [StoreId, MigrationId])),
    ?assertEqual(
       [read_only || _ <- Tables],
       [catch erpc:call(SomeNode, mnesia, table_info, [Table, access_mode])
        || Table <- Tables]),
    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, rollback_table_copy,
         [StoreId, MigrationId])),
    ?assertEqual(
       false,
       erpc:call(
         SomeNode,
         mnesia_to_khepri, is_migration_finished, [StoreId, MigrationId])),
    ?assertEqual(
       [read_write || _ <- Tables],
       [catch erpc:call(SomeNode, mnesia, table_info, [Table, access_mode])
        || Table <- Tables]),

    ok.

refuse_to_rollback_during_migration(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    ?assertEqual(
       {ok, StoreId},
       rpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    Table = my_record,
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, create_table,
         [Table, [{attributes, record_info(fields, my_record)},
                  {disc_copies, [Node1]}]])),
    ?assertEqual(
       {atomic, ok},
       rpc:call(
         Node1, mnesia, transaction,
         [fun() ->
                  I = 1,
                  Key = make_key(I),
                  Value = make_value(I),
                  mnesia:write(#my_record{key = Key, value = Value})
          end])),

    Parent = self(),

    ?assertEqual(
       false,
       erpc:call(
         Node1,
         mnesia_to_khepri, wait_for_migration,
         [StoreId, MigrationId, infinity])),

    _ = spawn_link(fun() ->
                           ?assertEqual(
                              ok,
                              erpc:call(
                                Node1, mnesia_to_khepri, copy_all_tables,
                                [StoreId, MigrationId,
                                 {controllable_converter, Parent}])),
                           erlang:unlink(Parent)
                   end),

    receive
        {init_copy_to_khepri, StoreId, Pid} ->
            _ = spawn_link(fun() ->
                                   Ret = erpc:call(
                                           Node1,
                                           mnesia_to_khepri,
                                           wait_for_migration,
                                           [StoreId, MigrationId, 1000]),
                                   Parent ! {?FUNCTION_NAME, Ret},
                                   erlang:unlink(Parent)
                           end),

            ?assertEqual(
               timeout,
               receive
                   {?FUNCTION_NAME, Ret} -> Ret
               end),

            ?assertEqual(
               set,
               catch erpc:call(Node1, mnesia, table_info, [Table, type])),
            ?assertEqual(
               {error, {in_flight, Pid}},
               erpc:call(
                 Node1, mnesia_to_khepri, rollback_table_copy,
                 [StoreId, MigrationId])),
            ?assertEqual(
               set,
               catch erpc:call(Node1, mnesia, table_info, [Table, type])),

            _ = spawn_link(fun() ->
                                   ct:pal("Waiting for migration to finish"),
                                   Ret = erpc:call(
                                           Node1,
                                           mnesia_to_khepri,
                                           wait_for_migration,
                                           [StoreId, MigrationId, infinity]),
                                   Parent ! {?FUNCTION_NAME, Ret},
                                   erlang:unlink(Parent)
                           end),

            ?assertEqual(
               timeout_from_testcase,
               receive
                   {?FUNCTION_NAME, Ret} -> Ret
               after 1000 ->
                         timeout_from_testcase
               end),

            Pid ! proceed
    end,

    ?assertEqual(
       true,
       receive
           {?FUNCTION_NAME, Ret} -> Ret
       after 5000 ->
                 timeout
       end),

    ok.

cannot_rollback_a_non_existing_migration(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    Nodes = lists:sort(maps:keys(PropsPerNode)),
    SomeNode = lists:nth(2, Nodes),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(SomeNode, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    helpers:cluster_mnesia_nodes(Nodes),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 {ok, StoreId},
                 erpc:call(Node, khepri, start, [RaSystem, StoreId]))
      end, Nodes),

    ?assertEqual(
       ok,
       erpc:call(
         SomeNode, mnesia_to_khepri, sync_cluster_membership, [StoreId])),

    Tables = [my_table1, my_table2],
    lists:foreach(
      fun(Table) ->
              ?assertEqual(
                 {atomic, ok},
                 erpc:call(
                   SomeNode, mnesia, create_table,
                   [Table, [{record_name, my_record},
                            {attributes, record_info(fields, my_record)},
                            {disc_copies, Nodes}]]))
      end, Tables),

    ?assertEqual(
       {atomic, ok},
       erpc:call(
         SomeNode, mnesia, transaction,
         [fun() ->
                  lists:foreach(
                    fun(I) ->
                            Key = make_key(I),
                            Value = make_value(I),
                            Record = #my_record{key = Key, value = Value},
                            lists:foreach(
                              fun(Table) ->
                                      mnesia:write(Table, Record, write)
                              end, Tables)
                    end, lists:seq(1, 1000))
          end])),

    ?assertEqual(
       [read_write || _ <- Tables],
       [catch erpc:call(SomeNode, mnesia, table_info, [Table, access_mode])
        || Table <- Tables]),
    ?assertEqual(
       {error, {no_such_migration, MigrationId}},
       erpc:call(
         SomeNode, mnesia_to_khepri, rollback_table_copy,
         [StoreId, MigrationId])),
    ?assertEqual(
       [read_write || _ <- Tables],
       [catch erpc:call(SomeNode, mnesia, table_info, [Table, access_mode])
        || Table <- Tables]),

    ok.

handle_fallback_with_mnesia_dirty_op_works(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    ?assertEqual(
       {ok, StoreId},
       erpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    Table = my_record,
    ?assertEqual(
       {atomic, ok},
       erpc:call(
         Node1, mnesia, create_table,
         [Table, [{attributes, record_info(fields, my_record)},
                  {disc_copies, [Node1]}]])),
    ?assertEqual(
       {atomic, ok},
       erpc:call(
         Node1, mnesia, transaction,
         [fun() ->
                  I = 1,
                  Key = make_key(I),
                  Value = make_value(I),
                  mnesia:write(#my_record{key = Key, value = Value})
          end])),

    MnesiaFun = fun() ->
                        I = 2,
                        Key = make_key(I),
                        Value = make_value(I),
                        Record = #my_record{key = Key, value = Value},
                        ok = mnesia:dirty_write(Record),
                        using_mnesia
                end,
    KhepriFun = fun() ->
                        I = 2,
                        Key = make_key(I),
                        Value = make_value(I),
                        case khepri:put(StoreId, [Table, Key], Value) of
                            ok  -> using_khepri;
                            Ret -> Ret
                        end
                end,

    Parent = self(),

    ?assertEqual(
       using_mnesia,
       erpc:call(
         Node1, mnesia_to_khepri, handle_fallback,
         [StoreId, MigrationId, MnesiaFun, KhepriFun])),

    _ = spawn_link(fun() ->
                           ?assertEqual(
                              ok,
                              erpc:call(
                                Node1, mnesia_to_khepri, copy_all_tables,
                                [StoreId, MigrationId,
                                 {controllable_converter, Parent}])),
                           Parent ! {?FUNCTION_NAME, copy_finished},
                           erlang:unlink(Parent)
                   end),

    receive
        {init_copy_to_khepri, StoreId, Pid} ->
            ?assertEqual(
               using_mnesia,
               erpc:call(
                 Node1, mnesia_to_khepri, handle_fallback,
                 [StoreId, MigrationId, MnesiaFun, KhepriFun])),

            Pid ! proceed
    end,

    ?assertEqual(
       ok,
       receive
           {?FUNCTION_NAME, copy_finished} -> ok
       after 5000 ->
                 timeout
       end),

    ?assertEqual(
       using_khepri,
       erpc:call(
         Node1, mnesia_to_khepri, handle_fallback,
         [StoreId, MigrationId, MnesiaFun, KhepriFun])),

    ok.

handle_fallback_with_mnesia_tx_works(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    ?assertEqual(
       {ok, StoreId},
       erpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    Table = my_record,
    ?assertEqual(
       {atomic, ok},
       erpc:call(
         Node1, mnesia, create_table,
         [Table, [{attributes, record_info(fields, my_record)},
                  {disc_copies, [Node1]}]])),
    ?assertEqual(
       {atomic, ok},
       erpc:call(
         Node1, mnesia, transaction,
         [fun() ->
                  I = 1,
                  Key = make_key(I),
                  Value = make_value(I),
                  mnesia:write(#my_record{key = Key, value = Value})
          end])),

    MnesiaFun = fun() ->
                        I = 2,
                        Key = make_key(I),
                        Value = make_value(I),
                        Record = #my_record{key = Key, value = Value},
                        Ret = mnesia:transaction(
                                fun() ->
                                        mnesia:write(Record)
                                end),
                        case Ret of
                            {atomic, ok} -> using_mnesia;
                            _            -> Ret
                        end
                end,
    KhepriFun = fun() ->
                        I = 2,
                        Key = make_key(I),
                        Value = make_value(I),
                        case khepri:put(StoreId, [Table, Key], Value) of
                            ok  -> using_khepri;
                            Ret -> Ret
                        end
                end,

    Parent = self(),

    ?assertEqual(
       using_mnesia,
       erpc:call(
         Node1, mnesia_to_khepri, handle_fallback,
         [StoreId, MigrationId, MnesiaFun, KhepriFun])),

    _ = spawn_link(fun() ->
                           ?assertEqual(
                              ok,
                              erpc:call(
                                Node1, mnesia_to_khepri, copy_all_tables,
                                [StoreId, MigrationId,
                                 {controllable_converter, Parent}])),
                           Parent ! {?FUNCTION_NAME, copy_finished},
                           erlang:unlink(Parent)
                   end),

    receive
        {init_copy_to_khepri, StoreId, Pid} ->
            ?assertEqual(
               using_mnesia,
               erpc:call(
                 Node1, mnesia_to_khepri, handle_fallback,
                 [StoreId, MigrationId, MnesiaFun, KhepriFun])),

            Pid ! proceed
    end,

    ?assertEqual(
       ok,
       receive
           {?FUNCTION_NAME, copy_finished} -> ok
       after 5000 ->
                 timeout
       end),

    ?assertEqual(
       using_khepri,
       erpc:call(
         Node1, mnesia_to_khepri, handle_fallback,
         [StoreId, MigrationId, MnesiaFun, KhepriFun])),

    ok.

handle_fallback_with_khepri_ret(Config) ->
    PropsPerNode = ?config(ra_system_props, Config),
    [Node1 | _] = lists:sort(maps:keys(PropsPerNode)),

    %% We assume all nodes are using the same Ra system name & store ID.
    #{ra_system := RaSystem} = maps:get(Node1, PropsPerNode),
    StoreId = RaSystem,
    MigrationId = store_id_to_mig_id(StoreId),

    ?assertEqual(
       {ok, StoreId},
       erpc:call(Node1, khepri, start, [RaSystem, StoreId])),

    Table = my_record,
    ?assertEqual(
       {atomic, ok},
       erpc:call(
         Node1, mnesia, create_table,
         [Table, [{attributes, record_info(fields, my_record)},
                  {disc_copies, [Node1]}]])),
    ?assertEqual(
       {atomic, ok},
       erpc:call(
         Node1, mnesia, transaction,
         [fun() ->
                  I = 1,
                  Key = make_key(I),
                  Value = make_value(I),
                  mnesia:write(#my_record{key = Key, value = Value})
          end])),

    MnesiaFun = fun() ->
                        I = 2,
                        Key = make_key(I),
                        Value = make_value(I),
                        Record = #my_record{key = Key, value = Value},
                        Ret = mnesia:transaction(
                                fun() ->
                                        mnesia:write(Record)
                                end),
                        case Ret of
                            {atomic, ok} -> using_mnesia;
                            _            -> Ret
                        end
                end,
    KhepriRet = using_khepri,

    Parent = self(),

    ?assertEqual(
       using_mnesia,
       erpc:call(
         Node1, mnesia_to_khepri, handle_fallback,
         [StoreId, MigrationId, MnesiaFun, KhepriRet])),

    _ = spawn_link(fun() ->
                           ?assertEqual(
                              ok,
                              erpc:call(
                                Node1, mnesia_to_khepri, copy_all_tables,
                                [StoreId, MigrationId,
                                 {controllable_converter, Parent}])),
                           Parent ! {?FUNCTION_NAME, copy_finished},
                           erlang:unlink(Parent)
                   end),

    receive
        {init_copy_to_khepri, StoreId, Pid} ->
            ?assertEqual(
               using_mnesia,
               erpc:call(
                 Node1, mnesia_to_khepri, handle_fallback,
                 [StoreId, MigrationId, MnesiaFun, KhepriRet])),

            Pid ! proceed
    end,

    ?assertEqual(
       ok,
       receive
           {?FUNCTION_NAME, copy_finished} -> ok
       after 5000 ->
                 timeout
       end),

    ?assertEqual(
       using_khepri,
       erpc:call(
         Node1, mnesia_to_khepri, handle_fallback,
         [StoreId, MigrationId, MnesiaFun, KhepriRet])),

    ok.

store_id_to_mig_id(StoreId) ->
    list_to_binary(atom_to_list(StoreId)).
