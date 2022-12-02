%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private

-module(m2k_table_copy).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
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
                  converter_mod :: mnesia_to_khepri:converter_mod() |
                                   {mnesia_to_khepri:converter_mod(), any()},
                  converter_mod_priv :: any() | undefined,
                  subscriber :: pid() | undefined,
                  progress :: #migration{}}).

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
                  {atomic, ok} ->
                      ?LOG_DEBUG(
                         "Mnesia->Khepri data copy: deleting Mnesia table "
                         "`~ts`",
                         [Table],
                         #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                      case mnesia:delete_table(Table) of
                          {atomic, ok} ->
                              ok;
                          {aborted, Reason2} ->
                              ?LOG_DEBUG(
                                 "Mnesia->Khepri data copy: failed to delete "
                                 "Mnesia table `~ts`: ~0p",
                                 [Table, Reason2],
                                 #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                              ok
                      end;
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
            m2k_subscriber:make_tables_readwrite(Tables),
            clear_migration_marker(StoreId, MigrationId, Progress),
            ok;
        {ok, #migration{progress = InFlight}} ->
            {error, InFlight};
        {error, {khepri, node_not_found, _}} ->
            {error, {no_such_migration, MigrationId}};
        {error, _} = Error ->
            Error
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
    State = #?MODULE{khepri_store = StoreId,
                     migration_id = MigrationId,
                     tables = Tables,
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

do_copy_data(#?MODULE{migration_id = MigrationId, tables = Tables} = State) ->
    ?LOG_INFO(
       "Mnesia->Khepri data copy: "
       "starting migration \"~ts\" from Mnesia to Khepri; tables: ~1p",
       [MigrationId, Tables],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),

    mark_tables_as_being_migrated(State),

    State1 = init_converter_mod(State),
    subscribe_to_mnesia_changes(State1),
    State2 = copy_from_mnesia_to_khepri(State1),
    State3 = final_sync_from_mnesia_to_khepri(State2),
    State4 = finish_converter_mod(State3),

    mark_tables_as_migrated(State4),

    ?LOG_INFO(
       "Mnesia->Khepri data copy: "
       "migration \"~ts\" from Mnesia to Khepri finished",
       [MigrationId],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),

    State4.

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

copy_from_mnesia_to_khepri(
  #?MODULE{khepri_store = StoreId,
           tables = Tables,
           converter_mod = Mod,
           converter_mod_priv = ModPriv} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: start actual data copy",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    CheckpointOptions = [{min, Tables},
                         {ram_overrides_dump, true}],
    case mnesia:activate_checkpoint(CheckpointOptions) of
        {ok, Checkpoint, _Nodes} ->
            ActualMod = actual_mod(Mod),
            Args = #{khepri_store => StoreId,
                     converter_mod => ActualMod,
                     converter_mod_priv => ModPriv,
                     table_copy_pid => self()},
            Ret = mnesia:backup_checkpoint(Checkpoint, Args, m2k_export),
            _ = mnesia:deactivate_checkpoint(Checkpoint),
            ModPriv1 = receive
                           {m2k_export, MP} -> MP
                       after 0 ->
                                 ModPriv
                       end,
            case Ret of
                ok ->
                    State#?MODULE{converter_mod_priv = ModPriv1};
                {error, {'EXIT', {error, {error, {_, {error, Reason}}}}}} ->
                    ?kmm_misuse(
                       converter_mod_exception,
                       #{converter_mod => Mod,
                         tables => Tables,
                         reason => Reason});
                Error ->
                    throw(
                      ?kmm_error(
                         converter_mod_error,
                         #{converter_mod => Mod,
                           error => Error}))
            end;
        Error ->
            throw(
              ?kmm_error(
                 failed_to_activate_mnesia_checkpoint,
                 #{tables => Tables,
                   error => Error}))
    end.

final_sync_from_mnesia_to_khepri(
  #?MODULE{subscriber = SubscriberPid,
           converter_mod = Mod,
           converter_mod_priv = ModPriv} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: final sync",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case m2k_subscriber:flush(SubscriberPid, ModPriv) of
        {ok, ModPriv1} ->
            State#?MODULE{converter_mod_priv = ModPriv1};
        Error ->
            throw(
              ?kmm_error(
                 converter_mod_error,
                 #{converter_mod => Mod,
                   error => Error}))
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
                    m2k_subscriber:make_tables_readwrite(Tables),
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
