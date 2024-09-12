%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @private

-module(m2k_table_copy).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("khepri/include/khepri.hrl").

-include("src/kmm_error.hrl").
-include("src/kmm_logging.hrl").

-export([start_link/1,
         proceed/1,
         is_migration_finished/2,
         cleanup/2,
         rollback/2,
         clear_migration_marker/4]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-ifdef(TEST).
-export([migration_recorded_state/2,
         marker_path/1]).
-endif.

-record(migration, {progress :: {in_flight, pid()} | finished,
                    tables :: [mnesia_to_khepri:mnesia_table()] | '_'}).

-record(?MODULE, {khepri_store :: khepri:store_id(),
                  migration_id :: mnesia_to_khepri:migration_id(),
                  tables :: [mnesia_to_khepri:mnesia_table()],
                  record_defs :: #{mnesia_to_khepri:mnesia_table() =>
                                   {atom(), arity()}},
                  converter_mod :: mnesia_to_khepri:converter_mod() |
                                   {mnesia_to_khepri:converter_mod(), any()},
                  converter_mod_priv :: any() | undefined,
                  subscriber :: pid() | undefined,
                  backup_pid :: pid() | undefined,
                  progress :: #migration{}}).

-define(PROJECTION_NAME, kmm_m2k_table_copy_projection).

proceed(SupPid) ->
    [{m2k_subscriber, SubscriberPid, _, _},
     {m2k_table_copy, TableCopyPid, _, _}] =
    lists:sort(supervisor:which_children(SupPid)),

    Ret = gen_server:call(
            TableCopyPid, {?FUNCTION_NAME, SubscriberPid}, infinity),
    case Ret of
        {exception, ?kmm_exception(_, _) = Exception} ->
            ?kmm_misuse(Exception);
        _ ->
            Ret
    end.

-spec is_migration_finished(StoreId, MigrationId) -> IsFinished when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      IsFinished :: boolean() | {in_flight, pid()} | undefined.

is_migration_finished(StoreId, MigrationId) when is_binary(MigrationId) ->
    %% If the Khepri store is not running, we can assume that the migration
    %% didn't take place yet.
    case khepri_cluster:is_store_running(StoreId) of
        true  -> is_migration_finished1(StoreId, MigrationId);
        false -> false
    end.

is_migration_finished1(StoreId, MigrationId) ->
    ProjectionName = ?PROJECTION_NAME,
    try
        case ets:lookup(ProjectionName, MigrationId) of
            [{_, #migration{progress = finished}}] ->
                true;
            [{_, #migration{progress = {in_flight, _} = InFlight}}] ->
                InFlight;
            [] ->
                false
        end
    catch
        error:badarg ->
            case setup_projection(StoreId, ProjectionName) of
                ok ->
                    is_migration_finished1(StoreId, MigrationId);
                %% Before Khepri v0.13.0, `khepri:register_projection/1,2,3`
                %% would return `{error, exists}` for projections which already
                %% exist.
                {error, exists} ->
                    is_migration_finished1(StoreId, MigrationId);
                %% In v0.13.0+, Khepri returns a `?khepri_error(..)` instead.
                {error, {khepri, projection_already_exists, _Info}} ->
                    is_migration_finished1(StoreId, MigrationId);
                Error ->
                    Key = {?MODULE, ?FUNCTION_NAME, StoreId},
                    case persistent_term:get(Key, undefined) of
                        Error ->
                            ok;
                        _ ->
                            ?LOG_INFO(
                               "Mnesia->Khepri fallback handling: couldn't "
                               "setup Khepri projection for migration "
                               "\"~ts\"; that's ok but expect slightly "
                               "slower versions of `is_migration_finished()` "
                               "and `handle_fallback()`~n~p",
                               [MigrationId, Error],
                               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                            persistent_term:put(Key, Error)
                    end,
                    is_migration_finished_slow(StoreId, MigrationId)
            end
    end.

setup_projection(StoreId, ProjectionName) ->
    %% In case this function is called many times concurrently, for instance
    %% because many processes use `mnesia_to_khepri:handle_fallback()' at the
    %% same time, we use a lock and check if the ETS table already exists
    %% inside the lock before registering the projection.
    %%
    %% This avoids that all these processes register the same projection many
    %% times, causing many Khepri/Ra commands to be sent to the leader.
    Lock = {{?MODULE, StoreId}, self()},
    global:set_lock(Lock, [node()]),
    try
        ProjectionName = ?PROJECTION_NAME,
        case ets:whereis(ProjectionName) of
            undefined ->
                ?LOG_DEBUG(
                   "Mnesia->Khepri data copy: setup Khepri projection "
                   "(name: \"~s\")",
                   [ProjectionName],
                   #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                PathPattern = marker_path(?KHEPRI_WILDCARD_STAR),
                Options = #{type => set, read_concurrency => true},
                ProjectionFun = fun(Path, Progress) ->
                                        {lists:last(Path), Progress}
                                end,
                Projection = khepri_projection:new(
                               ProjectionName, ProjectionFun, Options),
                khepri:register_projection(StoreId, PathPattern, Projection);
            _ ->
                ok
        end
    after
        global:del_lock(Lock, [node()])
    end.

is_migration_finished_slow(StoreId, MigrationId) ->
    Path = marker_path(MigrationId),
    case khepri:get_or(StoreId, Path, false) of
        {ok, #migration{progress = finished}}                  -> true;
        {ok, #migration{progress = {in_flight, _} = InFlight}} -> InFlight;
        {ok, false}                                            -> false;
        _                                                      -> undefined
    end.

-spec cleanup(StoreId, MigrationId) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Ret :: ok | {error, any()}.

cleanup(StoreId, MigrationId) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: deleting Mnesia tables after "
       "migration \"~ts\" finished",
       [MigrationId],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = marker_path(MigrationId),
    case khepri:get(StoreId, Path) of
        {ok, #migration{progress = finished,
                        tables = Tables}} ->
            do_cleanup(Tables);
        {ok, #migration{progress = InFlight}} ->
            {error, InFlight};
        {error, {khepri, node_not_found, _}} ->
            {error, {no_such_migration, MigrationId}};
        {error, _} = Error ->
            Error
    end.

do_cleanup(Tables) ->
    lists:foreach(
      fun(Table) ->
              ?LOG_DEBUG(
                 "Mnesia->Khepri data copy: marking Mnesia table `~ts` back "
                 "as read-write",
                 [Table],
                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
              case mnesia:change_table_access_mode(Table, read_write) of
                  Ret when Ret =:= {atomic, ok} orelse
                           Ret =:= {aborted,
                                    {already_exists, Table, read_write}} ->
                      ?LOG_DEBUG(
                         "Mnesia->Khepri data copy: deleting Mnesia table "
                         "`~ts`",
                         [Table],
                         #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                      case mnesia:delete_table(Table) of
                          {atomic, ok} ->
                              ok;
                          {aborted, {no_exists, _}} ->
                              ?LOG_DEBUG(
                                 "Mnesia->Khepri data copy: Mnesia table "
                                 "`~ts` already deleted",
                                 [Table],
                                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                              ok;
                          {aborted, Reason2} ->
                              ?LOG_DEBUG(
                                 "Mnesia->Khepri data copy: failed to delete "
                                 "Mnesia table `~ts`: ~0p",
                                 [Table, Reason2],
                                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                              ok
                      end;
                  {aborted, {no_exists, _}} ->
                      ?LOG_DEBUG(
                         "Mnesia->Khepri data copy: Mnesia table `~ts` "
                         "already deleted",
                         [Table],
                         #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                      ok;
                  {aborted, Reason1} ->
                      ?LOG_DEBUG(
                         "Mnesia->Khepri data copy: failed to mark Mnesia "
                         "table `~ts` as read-write: ~0p",
                         [Table, Reason1],
                         #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                      ok
              end
      end, Tables).

-spec rollback(StoreId, MigrationId) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Ret :: ok | {error, any()}.

rollback(StoreId, MigrationId) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: marking Mnesia tables back as read-write "
       "as part of migration \"~ts\" rollback",
       [MigrationId],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Path = marker_path(MigrationId),
    case khepri:get(StoreId, Path) of
        {ok, #migration{progress = finished,
                        tables = Tables} = Progress} ->
            make_tables_readwrite(Tables),
            clear_migration_marker(StoreId, MigrationId, Progress),
            wait_for_projected_record_deletion(MigrationId),
            ?assertEqual(false, is_migration_finished(StoreId, MigrationId)),
            ok;
        {ok, #migration{progress = InFlight}} ->
            {error, InFlight};
        {error, {khepri, node_not_found, _}} ->
            {error, {no_such_migration, MigrationId}};
        {error, _} = Error ->
            Error
    end.

wait_for_projected_record_deletion(MigrationId) ->
    Retry = try
                case ets:lookup(?PROJECTION_NAME, MigrationId) of
                    [_] -> true;
                    []  -> false
                end
            catch
                error:badarg ->
                    false
            end,
    case Retry of
        true ->
            timer:sleep(100),
            wait_for_projected_record_deletion(MigrationId);
        false ->
            ok
    end.

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% -------------------------------------------------------------------
%% `gen_server' callbacks.
%% -------------------------------------------------------------------

init(#{khepri_store := StoreId,
       migration_id := MigrationId,
       tables := Tables,
       converter_mod := Mod}) ->
    erlang:process_flag(trap_exit, true),
    Progress = migration_recorded_state(self(), Tables),
    RecordDefs = query_table_record_definitions(Tables),
    State = #?MODULE{khepri_store = StoreId,
                     migration_id = MigrationId,
                     tables = Tables,
                     record_defs = RecordDefs,
                     converter_mod = Mod,
                     progress = Progress},
    {ok, State}.

handle_call({proceed, SubscriberPid}, From, State) ->
    State1 = State#?MODULE{subscriber = SubscriberPid},
    try
        State2 = do_copy_data(State1),
        gen_server:reply(From, ok),

        {stop, normal, State2}
    catch
        throw:ok ->
            {stop, normal, ok, State1};
        throw:{already_started, OtherPid} = Reason ->
            Error = {error, Reason},
            ?LOG_INFO(
               "Mnesia->Khepri data copy already in progress by PID ~p",
               [OtherPid],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            {stop, normal, Error, State1};
        error:?kmm_exception(_, _) = Exception:Stacktrace ->
            ?LOG_ERROR(
               "Exception during Mnesia->Khepri data copy: ~0p~n~p",
               [Exception, Stacktrace],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            {stop, normal, {exception, Exception}, State1};
        throw:Reason:Stacktrace ->
            Error = {error, Reason},
            ?LOG_ERROR(
               "Failed to copy Mnesia->Khepri data: ~0p~n~p",
               [Error, Stacktrace],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            {stop, normal, Error, State1}
    end;
handle_call(Request, _From, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_call message: ~p",
       [Request],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {reply, undefined, State}.

handle_cast(Request, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_cast message: ~p",
       [Request],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_info message: ~p",
       [Msg],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {noreply, State}.

terminate(_Reason, State) ->
    clear_migration_marker(State),
    ok.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

query_table_record_definitions(Tables) ->
    query_table_record_definitions(Tables, #{}).

query_table_record_definitions([Table | Rest], RecordDefs) ->
    RecordName = mnesia:table_info(Table, record_name),
    Arity = mnesia:table_info(Table, arity),
    RecordDefs1 = RecordDefs#{Table => {RecordName, Arity}},
    query_table_record_definitions(Rest, RecordDefs1);
query_table_record_definitions([], RecordDefs) ->
    RecordDefs.

do_copy_data(#?MODULE{migration_id = MigrationId, tables = Tables} = State) ->
    ?LOG_INFO(
       "Mnesia->Khepri data copy: "
       "starting migration \"~ts\" from Mnesia to Khepri; tables: ~1p",
       [MigrationId, Tables],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),

    mark_tables_as_being_migrated(State),

    State1 = init_converter_mod(State),
    subscribe_to_mnesia_changes(State1),
    State2 = start_copy_from_mnesia_to_khepri(State1),
    State3 = handle_migration_records(State2),
    State4 = final_sync_from_mnesia_to_khepri(State3),
    State5 = finish_converter_mod(State4),

    mark_tables_as_migrated(State5),

    ?LOG_INFO(
       "Mnesia->Khepri data copy: "
       "migration \"~ts\" from Mnesia to Khepri finished",
       [MigrationId],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),

    State5.

init_converter_mod(
  #?MODULE{tables = Tables,
           khepri_store = StoreId,
           migration_id = MigrationId,
           converter_mod = Mod} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: initialize converter mod ~s for Mnesia "
       "tables: ~0p",
       [actual_mod(Mod), Tables],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    try
        Ret = case Mod of
                  {ActualMod, ModArgs} ->
                      ActualMod:init_copy_to_khepri(
                        StoreId, MigrationId, Tables, ModArgs);
                  _ ->
                      Mod:init_copy_to_khepri(
                        StoreId, MigrationId, Tables)
              end,
        case Ret of
            {ok, ModPriv} ->
                State#?MODULE{converter_mod_priv = ModPriv};
            Error ->
                throw(
                  ?kmm_error(
                     converter_mod_error,
                     #{converter_mod => Mod,
                       tables => Tables,
                       error => Error}))
        end
    catch
        Class:Reason:Stacktrace ->
            ?kmm_misuse(
               converter_mod_exception,
               #{converter_mod => Mod,
                 tables => Tables,
                 class => Class,
                 reason => Reason,
                 stacktrace => Stacktrace})
    end.

subscribe_to_mnesia_changes(
  #?MODULE{tables = Tables, subscriber = SubscriberPid}) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: subscribe to Mnesia changes",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case m2k_subscriber:subscribe(SubscriberPid, Tables) of
        ok ->
            ok;
        Error ->
            throw(
              ?kmm_error(
                 failed_to_subscribe_to_mnesia_changes,
                 #{tables => Tables,
                   error => Error}))
    end.

start_copy_from_mnesia_to_khepri(#?MODULE{tables = Tables} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: start actual data copy",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    CheckpointOptions = [{min, Tables},
                         {ram_overrides_dump, true}],
    case mnesia:activate_checkpoint(CheckpointOptions) of
        {ok, Checkpoint, _Nodes} ->
            Self = self(),
            Args = #{table_copy_pid => Self},
            BackupPid = spawn_link(
                          fun() ->
                                  Ret = mnesia:backup_checkpoint(
                                          Checkpoint, Args, m2k_export),
                                  _ = mnesia:deactivate_checkpoint(Checkpoint),
                                  Self ! {self(), done, Ret},
                                  unlink(Self),
                                  exit(normal)
                          end),
            State#?MODULE{backup_pid = BackupPid};
        Error ->
            throw(
              ?kmm_error(
                 failed_to_activate_mnesia_checkpoint,
                 #{tables => Tables,
                   error => Error}))
    end.

handle_migration_records(
  #?MODULE{backup_pid = BackupPid,
           converter_mod = Mod,
           converter_mod_priv = ModPriv,
           tables = Tables} = State) ->
    receive
        {m2k_export, ExportPid, handle_record, Table, Record} ->
            {State2, Reply} =
            try
                ActualMod = actual_mod(Mod),
                Ret = case Record of
                          {_RecordName, Key} ->
                              ActualMod:delete_from_khepri(
                                Table, Key, ModPriv);
                          _ ->
                              ActualMod:copy_to_khepri(
                                Table, Record, ModPriv)
                      end,
                case Ret of
                    {ok, ModPriv1} ->
                        State1 = State#?MODULE{converter_mod_priv = ModPriv1},
                        {State1, ok};
                    Error ->
                        {State, Error}
                end
            catch
                Class:Reason:Stacktrace ->
                    Exception = ?kmm_exception(
                                   converter_mod_exception,
                                   #{converter_mod => Mod,
                                     tables => Tables,
                                     class => Class,
                                     reason => Reason,
                                     stacktrace =>
                                     Stacktrace}),
                    {State, {error, Exception}}
            end,
            ExportPid ! {self(), record_handled, Reply},
            handle_migration_records(State2);
        {BackupPid, done, Ret} ->
            case Ret of
                ok ->
                    State;
                {error,
                 {'EXIT',
                  {error,
                   {error,
                    {_, {error, ?kmm_exception(_, _) = Exception}}}}}} ->
                    ?kmm_misuse(Exception);
                Error ->
                    throw(
                      ?kmm_error(
                         converter_mod_error,
                         #{converter_mod => Mod,
                           error => Error}))
            end;
        {'EXIT', BackupPid, Reason} ->
            throw(
              ?kmm_error(
                 backup_process_error,
                 #{converter_mod => Mod,
                   error => Reason}))
    after
        15_000 ->
            throw(
              ?kmm_error(
                 converter_mod_error,
                 #{converter_mod => Mod,
                   error => timeout}))
    end.

final_sync_from_mnesia_to_khepri(
  #?MODULE{tables = Tables,
           subscriber = SubscriberPid} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: final sync",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    %% Switch all tables to read-only. All concurrent and future Mnesia
    %% transactions involving a write to one of them will fail with the
    %% `{no_exists, Table}' exception.
    make_tables_readonly(Tables),

    try
        Events = m2k_subscriber:drain(SubscriberPid),
        consume_mnesia_events(Events, State)
    catch
        Class:Reason:Stacktrace ->
            make_tables_readwrite(Tables),
            erlang:raise(Class, Reason, Stacktrace)
    end.

finish_converter_mod(
  #?MODULE{tables = Tables,
           converter_mod = Mod,
           converter_mod_priv = ModPriv} = State) ->
    ActualMod = actual_mod(Mod),
    case erlang:function_exported(ActualMod, finish_copy_to_khepri, 1) of
        true ->
            try
                _ = ActualMod:finish_copy_to_khepri(ModPriv),
                State#?MODULE{converter_mod_priv = undefined}
            catch
                Class:Reason:Stacktrace ->
                    make_tables_readwrite(Tables),
                    ?kmm_misuse(
                       converter_mod_exception,
                       #{converter_mod => Mod,
                         tables => Tables,
                         class => Class,
                         reason => Reason,
                         stacktrace => Stacktrace})
            end;
        false ->
            State
    end.

actual_mod({Mod, _ModArgs}) when is_atom(Mod) ->
    Mod;
actual_mod(Mod) when is_atom(Mod) ->
    Mod.

mark_tables_as_being_migrated(
  #?MODULE{khepri_store = StoreId,
           migration_id = MigrationId,
           progress = Progress}) ->
    Path = marker_path(MigrationId),
    case khepri:create(StoreId, Path, Progress) of
        ok ->
            ok;

        {error,
         {khepri, mismatching_node,
          #{node_props := #{data :=
                            #migration{
                               progress = finished}}}}} ->
            throw(ok);

         {error,
          {khepri, mismatching_node,
           #{node_props := #{data :=
                             #migration{
                                progress = {in_flight, OtherPid}}}}}} ->
            throw({already_started, OtherPid});

        {error, Reason} ->
            throw(Reason)
    end.

mark_tables_as_migrated(
  #?MODULE{khepri_store = StoreId,
           migration_id = MigrationId,
           progress = Progress}) ->
    Condition = #if_data_matches{pattern = Progress},
    Path = marker_path(#if_all{conditions = [MigrationId, Condition]}),
    Progress1 = Progress#migration{progress = finished},
    case khepri:update(StoreId, Path, Progress1) of
        ok              -> ok;
        {error, Reason} -> throw(Reason)
    end.

clear_migration_marker(
  #?MODULE{khepri_store = StoreId,
           migration_id = MigrationId,
           progress = Progress}) ->
    clear_migration_marker(StoreId, MigrationId, Progress).

clear_migration_marker(StoreId, MigrationId, Pid, Tables) ->
    Progress = migration_recorded_state(Pid, Tables),
    Condition = #if_data_matches{pattern = Progress},
    Path = marker_path(#if_all{conditions = [MigrationId, Condition]}),
    _ = khepri:delete(StoreId, Path),
    ok.

clear_migration_marker(StoreId, MigrationId, Progress) ->
    Condition = #if_data_matches{pattern = Progress},
    Path = marker_path(#if_all{conditions = [MigrationId, Condition]}),
    _ = khepri:delete(StoreId, Path),
    ok.

migration_recorded_state(Pid, Tables) when is_pid(Pid) ->
    #migration{progress = {in_flight, Pid},
               tables = Tables}.

marker_path(PathComponent) ->
    ['__khepri_mnesia_migration', ?MODULE, PathComponent].

make_tables_readonly(Tables) ->
    make_tables_readonly(Tables, []).

make_tables_readonly([Table | Rest], AlreadyMarked) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: mark Mnesia table `~ts` as read-only",
       [Table],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case mnesia:change_table_access_mode(Table, read_only) of
        {atomic, ok} ->
            make_tables_readonly(Rest, [Table | AlreadyMarked]);
        {aborted, {already_exists, _, read_only}} ->
            make_tables_readonly(Rest, [Table | AlreadyMarked]);
        {aborted, _} = Error ->
            _ = make_tables_readwrite(AlreadyMarked),
            throw(Error)
    end;
make_tables_readonly([], _AlreadyMarked) ->
    ok.

make_tables_readwrite([Table | Rest]) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: mark Mnesia table `~ts` as read-write after "
       "a failed copy or a rollback",
       [Table],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    _ = mnesia:change_table_access_mode(Table, read_write),
    make_tables_readwrite(Rest);
make_tables_readwrite([]) ->
    ok.

consume_mnesia_events(
  Events,
  #?MODULE{tables = Tables,
           converter_mod = Mod,
           converter_mod_priv = ModPriv} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: consuming ~b Mnesia events from tables ~0p",
       [length(Events), Tables],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    ActualMod = actual_mod(Mod),
    ModPriv1 = consume_mnesia_events1(Events, ActualMod, ModPriv, State),
    State#?MODULE{converter_mod_priv = ModPriv1}.

consume_mnesia_events1(
  [{put, Table, Record} | Rest], Mod, ModPriv, State) ->
    ModPriv2 = case Mod:copy_to_khepri(Table, Record, ModPriv) of
                   {ok, ModPriv1} -> ModPriv1;
                   Error          -> throw(Error)
               end,
    Remaining = length(Rest),
    if
        Remaining rem 100 =:= 0 ->
            ?LOG_DEBUG(
               "Mnesia->Khepri data copy: ~b Mnesia events left",
               [Remaining],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN});
        true ->
            ok
    end,
    consume_mnesia_events1(Rest, Mod, ModPriv2, State);
consume_mnesia_events1(
  [{delete, Table, Key} | Rest], Mod, ModPriv, State) ->
    ModPriv2 = case Mod:delete_from_khepri(Table, Key, ModPriv) of
                   {ok, ModPriv1} ->
                       ModPriv1;
                   Error ->
                       throw(Error)
               end,
    Remaining = length(Rest),
    if
        Remaining rem 100 =:= 0 ->
            ?LOG_DEBUG(
               "Mnesia->Khepri data copy: ~b Mnesia events left",
               [Remaining],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN});
        true ->
            ok
    end,
    consume_mnesia_events1(Rest, Mod, ModPriv2, State);
consume_mnesia_events1([], _Mod, ModPriv, _State) ->
    ModPriv.
