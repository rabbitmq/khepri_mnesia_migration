%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @doc Tools to migrate from Mnesia to Khepri.
%%
%% The migration from Mnesia to Khepri implemented in this module is divided in
%% two distinct parts:
%% <ol>
%% <li>the cluster membership</li>
%% <li>the tables content</li>
%% </ol>
%%
%% Both parts can be used independently.
%%
%% == Cluster membership synchronization ==
%%
%% For the first part, {@link sync_cluster_membership/0} and {@link
%% sync_cluster_membership/1} ensure the default/specified Khepri store has the
%% same cluster members as Mnesia.
%%
%% All "instances" of the Khepri store on unclustered nodes will be reset
%% except one. The selected surviving Khepri store is determined using several
%% heuristics which are explained in {@link sync_cluster_membership/1}.
%%
%% == Tables copy ==
%%
%% For the second part, {@link copy_tables/3}, {@link copy_tables/4}, {@link
%% copy_all_tables/2} and {@link copy_all_tables/3} take care of copying
%% records to the designated Khepri store.
%%
%% The functions take a converter module which is responsible for actually
%% writing each record to wherever it wants in the Khepri store. This allows
%% the caller to filter and convert the records. The converter module
%% interface is defined by the {@link mnesia_to_khepri_converter} behavior.
%% There is an example converter module called {@link
%% mnesia_to_khepri_example_converter} provided.
%%
%% The copy works while Mnesia tables are still being used and updated. To
%% allow that, the copy follows several stages which are explained in {@link
%% copy_tables/4}.
%%
%% The functions {@link is_migration_finished/1}, {@link
%% is_migration_finished/2}, {@link wait_for_migration/2} and {@link
%% wait_for_migration/3} can be used to follow an on-going copy of the tables.
%%
%% Finally to help with the use of the Mnesia tables and Khepri store
%% concurrently with a tables copy, you can use {@link handle_fallback/3} and
%% {@link handle_fallback/4}.

-module(mnesia_to_khepri).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_error.hrl").
-include("src/kmm_logging.hrl").

-export([sync_cluster_membership/0, sync_cluster_membership/1,
         copy_tables/3, copy_tables/4,
         copy_all_tables/2, copy_all_tables/3,
         is_migration_finished/1, is_migration_finished/2,
         wait_for_migration/2, wait_for_migration/3,
         cleanup_after_table_copy/1, cleanup_after_table_copy/2,
         rollback_table_copy/1, rollback_table_copy/2,
         handle_fallback/3, handle_fallback/4]).

-type migration_id() :: binary().
%% MigrationId of a migration.
%%
%% This is used to semantically identify a migration that covers a set of
%% Mnesia tables and an associated converter module.
%%
%% A migration is idempotent, based on this identifier. In other words, a
%% migration identifier by this identifier can happen only once, and there
%% can't be concurrent migration processes with that same identifier.

-type mnesia_table() :: atom().
%% The name of a Mnesia table.
%%
%% This is the same type as `mnesia:table()' which is not exported
%% unfortunately.

-type converter_mod() :: module() | {module(), any()}.
%% A converter module, possibly with a private term to initliaze it.
%%
%% A converter module is a module implementing the {@link
%% mnesia_to_khepri_converter} behavior. The private term is passed as is to
%% its @{link mnesia_to_khepri_converter:init_copy_to_khepri/3} callback.

-export_type([migration_id/0,
              mnesia_table/0,
              converter_mod/0]).

%% -------------------------------------------------------------------
%% Cluster membership.
%% -------------------------------------------------------------------

-spec sync_cluster_membership() -> ok.
%% @doc Ensures the default Khepri store has the same members as the Mnesia
%% cluster.
%%
%% @see sync_cluster_membership/1.

sync_cluster_membership() ->
    StoreId = khepri_cluster:get_default_store_id(),
    sync_cluster_membership(StoreId).

-spec sync_cluster_membership(StoreId) -> ok when
      StoreId :: khepri:store_id().
%% @doc Ensures the Khepri store named `StoreId' has the same members as the
%% Mnesia cluster.
%%
%% The synchronization is split into the following steps:
%% <ol>
%% <li>Mnesia is queried to list all its members. We ensure that all Mnesia
%% members are running at this point: if some members are down, there is
%% little chance that Khepri will work on those anyway.</li>
%% <li>All Mnesia members are queried to learn about the membership status of
%% all "instances" of the Khepri store named `StoreId'.</li>
%% <li>Among all instances of the Khepri store, one instance is selected to be
%% the one that will be joined by other nodes. The selection process is
%% described below.</li>
%% <li>Other nodes are reset, meaning their data is dropped, and they are
%% added to the selected Khepri store.</li>
%% </ol>
%%
%% The synchronization process has to select a store instance that will grow
%% and other store instances that will be reset. The criterias below are
%% evaluated to sort the list of store instances, then the first instance in
%% that list is selected as the winning instance.
%%
%% Criterias are evaluated in order. For a given criteria, if both sides are
%% equal, the next criteria is evaluated until one criteria gives an instance
%% "greater" than another.
%%
%% Here is the ordered list of criterias:
%% <ol>
%% <li>A Khepri store instance with more members gets precedence.</li>
%% <li>Then, a Khepri store instance with more tree nodes (more records) gets
%% precedence.</li>
%% <li>Then, a Khepri store instance where a member's Erlang node runs for the
%% longest time gets precedence.</li>
%% <li>Then, a Khepri store instance where its members list sorts before
%% another members list gets precedence.</li>
%% </ol>
%%
%% Here are some examples:
%% <ul>
%% <li>A Khepri store instance with 3 members will be selected before another
%% Khepri store instance with 1 member.</li>
%% <li>If they have the same number of members, a Khepri store instance with
%% 125 tree nodes will be selected before another Khepri store instance with
%% 34 tree nodes.</li>
%% <li>If they have the same number of members and the same number of tree
%% nodes, a Khepri store instance where the oldest Erlang node runs for 67
%% days will be selected before another Khepri store instance where the oldest
%% Erlang node runs for 3 days.</li>
%% </ul>

sync_cluster_membership(StoreId) ->
    case m2k_cluster_sync_sup:prepare_worker(StoreId) of
        {ok, Pid} when is_pid(Pid) -> m2k_cluster_sync:proceed(Pid);
        {error, _} = Error         -> Error
    end.

%% -------------------------------------------------------------------
%% Tables copy.
%% -------------------------------------------------------------------

-spec copy_tables(MigrationId, Tables, Mod) -> Ret when
      MigrationId :: migration_id(),
      Tables :: [mnesia_table()],
      Mod :: converter_mod() | {converter_mod(), ModArgs},
      ModArgs :: any(),
      Ret :: ok | {error, any()}.
%% @doc Copies records from Mnesia tables `Tables' to the default Khepri
%% store.
%%
%% @see copy_tables/3.

copy_tables(MigrationId, Tables, Mod) ->
    StoreId = khepri_cluster:get_default_store_id(),
    copy_tables(StoreId, MigrationId, Tables, Mod).

-spec copy_tables(StoreId, MigrationId, Tables, Mod) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: migration_id(),
      Tables :: [mnesia_table()],
      Mod :: converter_mod() | {converter_mod(), ModArgs},
      ModArgs :: any(),
      Ret :: ok | {error, any()}.
%% @doc Copies records from Mnesia tables `Tables' to the Khepri store named
%% `StoreId'.
%%
%% The converter module `Mod' is responsible for storing each Mnesia record in
%% the Khepri store. How it is called is described below. {@link
%% mnesia_to_khepri_example_converter} can be used as the default converter
%% module or as an example to write a new one.
%%
%% The copy is split into the following steps:
%% <ol>
%% <li>The PID of the Erlang process working on the copy is stored in the
%% Khepri store. This serves as an indicator that tables are being copied and
%% this prevents concurrent copy from happening.</li>
%% <li>The converter module state is initialized using its
%% `Mod:init_copy_to_khepri/3' function, or `Mod:init_copy_to_khepri/4' if
%% `ModArgs' is set.</li>
%% <li>The copy process subscribes to changes from Mnesia.</li>
%% <li>The copy process copies records from Mnesia using its Backup &amp;
%% Restore API. The process uses `Mod:copy_to_khepri/3' for each Mnesia
%% record.</li>
%% <li>The copy process marks all involved Mnesia tables as read-only.</li>
%% <li>The copy process consumes all Mnesia events it received during the
%% initial copy and calls `Mod:copy_to_khepri/3' and
%% `Mod:delete_from_khepri/3' to update the Khepri store accordingly.</li>
%% <li>The copy process calls `Mod:finish_copy_to_khepri/1' to let the
%% converter module do any cleanups.</li>
%% <li>The "migration in progress" marker is removed from the Khepri
%% store.</li>
%% </ol>
%%
%% Copied Mnesia tables continue to be available during the process, except
%% after they are marked as read-only. See {@link handle_fallback/2} and {@link
%% handle_fallback/3} for helpers to use Mnesia while records are being copied.

copy_tables(StoreId, MigrationId, Tables, Mod)
  when is_binary(MigrationId) ->
    ExistingTables = filter_out_non_existing_tables(Tables),
    copy_tables1(StoreId, MigrationId, ExistingTables, Mod).

copy_tables1(_StoreId, _MigrationId, [], _Mod) ->
    ok;
copy_tables1(StoreId, MigrationId, Tables, Mod)
  when is_binary(MigrationId) andalso
       is_list(Tables) andalso
       Tables =/= [] andalso
       (is_atom(Mod) orelse
        (is_tuple(Mod) andalso size(Mod) =:= 2 andalso
         is_atom(element(1, Mod)))) ->
    Ret = m2k_table_copy_sup_sup:prepare_workers_sup(
            StoreId, MigrationId, Tables, Mod),
    case Ret of
        {ok, Pid} when is_pid(Pid) ->
            copy_tables2(StoreId, MigrationId, Tables, Mod, Pid);
        {error, _} = Error ->
            Error
    end.

filter_out_non_existing_tables(Tables) ->
    %% This filtering is not atomic w.r.t. the upcoming copy process, but a
    %% table could be added or removed at any time anyway.
    lists:filter(
      fun(Table) ->
              try
                  _ = mnesia:table_info(Table, type),
                  true
              catch
                  exit:{aborted, {no_exists, Table, type}} ->
                      %% The table doesn't exist in the first place, there is
                      %% nothing to migrate.
                      ?LOG_DEBUG(
                         "Mnesia->Khepri data copy: Table `~ts` does not "
                         "exist, skipping its migration",
                         [Table],
                         #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                      false
              end
      end,
      Tables).

copy_tables2(StoreId, MigrationId, Tables, Mod, Pid) ->
    case m2k_table_copy:proceed(Pid) of
        ok ->
            ok;
        {error, {already_started, OtherPid}} ->
            MRef = erlang:monitor(process, OtherPid),
            receive
                {'DOWN', MRef, process, OtherPid, normal} ->
                    copy_tables1(StoreId, MigrationId, Tables, Mod);
                {'DOWN', MRef, process, OtherPid, noproc} ->
                    %% The previous migration crashed or the node was killed
                    %% for instance. The `in_flight' marker is still there
                    %% but the process is gone. We can clear the marker and
                    %% retry.
                    m2k_table_copy:clear_migration_marker(
                      StoreId, MigrationId, OtherPid, '_'),
                    copy_tables1(StoreId, MigrationId, Tables, Mod);
                {'DOWN', MRef, process, OtherPid, Info} ->
                    {error, Info}
            end;
        {error, _} = Error ->
            Error
    end.

-spec copy_all_tables(MigrationId, Mod) -> Ret when
      MigrationId :: migration_id(),
      Mod :: converter_mod() | {converter_mod(), ModArgs},
      ModArgs :: any(),
      Ret :: ok | {error, any()}.
%% @doc Copies records from all Mnesia tables to the default Khepri store.
%%
%% @see copy_all_tables/3.

copy_all_tables(MigrationId, Mod) ->
    StoreId = khepri_cluster:get_default_store_id(),
    copy_all_tables(StoreId, MigrationId, Mod).

-spec copy_all_tables(StoreId, MigrationId, Mod) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: migration_id(),
      Mod :: converter_mod() | {converter_mod(), ModArgs},
      ModArgs :: any(),
      Ret :: ok | {error, any()}.
%% @doc Copies records from all Mnesia tables to the Khepri store named
%% `StoreId'.
%%
%% @see copy_tables/4.

copy_all_tables(StoreId, MigrationId, Mod) ->
    Tables = list_all_tables(),
    copy_tables(StoreId, MigrationId, Tables, Mod).

-spec list_all_tables() -> Tables when
      Tables :: [mnesia_table()].
%% @private

list_all_tables() ->
    Tables0 = lists:sort(mnesia:system_info(tables)),
    Tables1 = Tables0 -- [schema],
    Tables1.

-spec is_migration_finished(MigrationId) -> Migrated when
      MigrationId :: mnesia_to_khepri:migration_id(),
      Migrated :: boolean() | {in_flight, pid()} | undefined.
%% @doc Returns the migration status of the specified migration identifier.
%%
%% The default Khepri store is queried to get the migration status. It must
%% correspond to the Khepri store passed to {@link copy_tables/3} and similar
%% functions.
%%
%% @see is_migration_finished/2.

is_migration_finished(MigrationId) ->
    StoreId = khepri_cluster:get_default_store_id(),
    is_migration_finished(StoreId, MigrationId).

-spec is_migration_finished(StoreId, MigrationId) -> Migrated when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Migrated :: boolean() | {in_flight, pid()} | undefined.
%% @doc Returns the migration status of the specified migration identifier.
%%
%% The Khepri store named `StoreId' is queried to get the migration status. It
%% must correspond to the Khepri store passed to {@link copy_tables/3} and
%% similar functions.
%%
%% @returns `true' if the migration is finished, `{in_flight, Pid}' if the
%% migration is in progress and handled by the `Pid' process, `false' if the
%% migration has not started or `undefined' if the query of the Khepri store
%% where the status is recorded failed.

is_migration_finished(StoreId, MigrationId) when is_binary(MigrationId) ->
    m2k_table_copy:is_migration_finished(StoreId, MigrationId).

-spec wait_for_migration(MigrationId, Timeout) -> Ret when
      MigrationId :: mnesia_to_khepri:migration_id(),
      Timeout :: timeout(),
      Ret :: boolean() | timeout.
%% @doc Waits for migration `MigrationId' to be finish.
%%
%% The default Khepri store is queried to get the migration status. It must
%% correspond to the Khepri store passed to {@link copy_tables/3} and similar
%% functions.
%%
%% @see wait_for_migration/3.

wait_for_migration(MigrationId, Timeout) ->
    StoreId = khepri_cluster:get_default_store_id(),
    wait_for_migration(StoreId, MigrationId, Timeout).

-spec wait_for_migration(StoreId, MigrationId, Timeout) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Timeout :: timeout(),
      Ret :: boolean() | timeout.
%% @doc Waits for migration `MigrationId' to be finish.
%%
%% If the migration has not started, it returns `false' immediately.
%%
%% If the migration is finished, it returns `true' immediately.
%%
%% If the migration is in progress or the status is undefined (see {@link
%% is_migration_finished/3}), it waits until the status is known to be
%% "finished" or "not started" or until `Timeout' milliseconds.
%%
%% The Khepri store named `StoreId' is queried to get the migration status. It
%% must correspond to the Khepri store passed to {@link copy_tables/3} and
%% similar functions.

wait_for_migration(_StoreId, MigrationId, 0)
  when is_binary(MigrationId) ->
    timeout;
wait_for_migration(StoreId, MigrationId, Timeout)
  when is_binary(MigrationId) ->
    T0 = khepri_utils:start_timeout_window(Timeout),
    case is_migration_finished(StoreId, MigrationId) of
        true ->
            true;
        false ->
            false;
        _ ->
            timer:sleep(250),
            NewTimeout = khepri_utils:end_timeout_window(Timeout, T0),
            wait_for_migration(StoreId, MigrationId, NewTimeout)
    end.

-spec cleanup_after_table_copy(MigrationId) -> Ret when
      MigrationId :: mnesia_to_khepri:migration_id(),
      Ret :: ok | {error, any()}.
%% @doc Performs any cleanups after a migration has finished.
%%
%% @see cleanup_after_table_copy/2.

cleanup_after_table_copy(MigrationId) ->
    StoreId = khepri_cluster:get_default_store_id(),
    cleanup_after_table_copy(StoreId, MigrationId).

-spec cleanup_after_table_copy(StoreId, MigrationId) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Ret :: ok | {error, any()}.
%% @doc Performs any cleanups after a migration has finished.
%%
%% Currently this includes the deletion of the copied Mnesia tables.
%%
%% This is a separate step from {@link copy_tables/4} because the caller might
%% want to record some post-migration states before comitting to delete the
%% Mnesia tables.

cleanup_after_table_copy(StoreId, MigrationId)
  when is_binary(MigrationId) ->
    m2k_table_copy:cleanup(StoreId, MigrationId).

-spec rollback_table_copy(MigrationId) -> Ret when
      MigrationId :: mnesia_to_khepri:migration_id(),
      Ret :: ok | {error, any()}.
%% @doc Rolls back a migration.
%%
%% @see rollback_table_copy/2.

rollback_table_copy(MigrationId) ->
    StoreId = khepri_cluster:get_default_store_id(),
    rollback_table_copy(StoreId, MigrationId).

-spec rollback_table_copy(StoreId, MigrationId) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Ret :: ok | {error, any()}.
%% @doc Rolls back a migration.
%%
%% This function puts Mnesia tables back to read-write mode and deletes the
%% "migration in progress" marker.
%%
%% Rollback is not possible once {@link cleanup_after_table_copy/2} is used.

rollback_table_copy(StoreId, MigrationId)
  when is_binary(MigrationId) ->
    m2k_table_copy:rollback(StoreId, MigrationId).

-spec handle_fallback(MigrationId, MnesiaFun, KhepriFunOrRet) ->
    Ret when
      MigrationId :: mnesia_to_khepri:migration_id(),
      MnesiaFun :: fun(() -> Ret),
      KhepriFunOrRet :: fun(() -> Ret) | Ret,
      Ret :: any().
%% @doc Runs `MnesiaFun' or evaluates `KhepriFunOrRet' depending on the status
%% of the migration.
%%
%% The default Khepri store is queried to get the migration status. It must
%% correspond to the Khepri store passed to {@link copy_tables/3} and similar
%% functions.
%%
%% @see handle_fallback/4.

handle_fallback(MigrationId, MnesiaFun, KhepriFunOrRet) ->
    StoreId = khepri_cluster:get_default_store_id(),
    handle_fallback(StoreId, MigrationId, MnesiaFun, KhepriFunOrRet).

-spec handle_fallback(StoreId, MigrationId, MnesiaFun, KhepriFunOrRet) ->
    Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      MnesiaFun :: fun(() -> Ret),
      KhepriFunOrRet :: fun(() -> Ret) | Ret,
      Ret :: any().
%% @doc Runs `MnesiaFun' or evaluates `KhepriFunOrRet' depending on the status
%% of the migration.
%%
%% If the migration is finished it executes `KhepriFunOrRet' if it is a
%% function with an arity of 0 or returns the term directly otherwise.
%%
%% If the migration is not finished, the function tries to execute
%% `MnesiaFun'. It should fail by returning or raising `{aborted, {no_exists,
%% Table}}'. When this happens, the function waits for the migration to finish
%% using {@link wait_for_table_migration/3}, then it starts again.
%%
%% The Khepri store named `StoreId' is queried to get the migration status. It
%% must correspond to the Khepri store passed to {@link copy_tables/3} and
%% similar functions.

-define(UNKNOWN_TABLE, 0).
-define(CONSIDER_INFINITE_LOOP_AFTER, 100).

handle_fallback(StoreId, MigrationId, MnesiaFun, KhepriFunOrRet)
  when is_binary(MigrationId) andalso is_function(MnesiaFun, 0) ->
    handle_fallback(StoreId, MigrationId, MnesiaFun, KhepriFunOrRet, 1).

handle_fallback(StoreId, MigrationId, MnesiaFun, KhepriFunOrRet, Attempts)
  when Attempts < ?CONSIDER_INFINITE_LOOP_AFTER ->
    case is_migration_finished(StoreId, MigrationId) of
        true when is_function(KhepriFunOrRet, 0) ->
            KhepriFunOrRet();
        true ->
            KhepriFunOrRet;
        _ ->
            try
                case MnesiaFun() of
                    {aborted, NoExists1}
                      when is_tuple(NoExists1) andalso
                           element(1, NoExists1) =:= no_exists ->
                        {_, Stacktrace1} = erlang:process_info(
                                             self(), current_stacktrace),
                        log_no_exists_error(NoExists1, Stacktrace1),

                        _ = wait_for_migration(StoreId, MigrationId, 1000),
                        handle_fallback(
                          StoreId, MigrationId, MnesiaFun, KhepriFunOrRet,
                          Attempts + 1);
                    Ret ->
                        Ret
                end
            catch
                _:{_aborted, NoExists2}:Stacktrace2
                  when is_tuple(NoExists2) andalso
                       element(1, NoExists2) =:= no_exists ->
                    log_no_exists_error(NoExists2, Stacktrace2),

                    _ = wait_for_migration(StoreId, MigrationId, 1000),
                    handle_fallback(
                      StoreId, MigrationId, MnesiaFun, KhepriFunOrRet,
                      Attempts + 1)
            end
    end;
handle_fallback(
  _StoreId, _MigrationId, MnesiaFun, _KhepriFunOrRet, Attempts) ->
    ?LOG_WARNING(
       "Mnesia->Khepri fallback handling: Mnesia function failed ~b times. "
       "Possibly an infinite retry loop; trying one last time",
       [Attempts - 1]),
    MnesiaFun().

log_no_exists_error(NoExists, Stacktrace) ->
    case table_name_from_no_exists(NoExists) of
        ?UNKNOWN_TABLE ->
            ?LOG_DEBUG(
               "Mnesia->Khepri fallback handling: Mnesia function failed "
               "because at least one table is missing or read-only. "
               "Migration could be in progress; waiting for migration to "
               "progress and trying again~n"
               "~p",
               [Stacktrace]),
            ok;
        Table ->
            ?LOG_DEBUG(
               "Mnesia->Khepri fallback handling: Mnesia function failed "
               "because table `~ts` is missing or read-only. Migration "
               "could be in progress; waiting for migration to progress "
               "and trying again~n"
               "~p",
               [Table, Stacktrace]),
            ok
    end.

table_name_from_no_exists({no_exists, Table}) when is_atom(Table) ->
    Table;
table_name_from_no_exists({no_exists, [Table | _]}) when is_atom(Table) ->
    Table;
table_name_from_no_exists({no_exists, Table, _}) when is_atom(Table) ->
    Table;
table_name_from_no_exists(_) ->
    ?UNKNOWN_TABLE.
