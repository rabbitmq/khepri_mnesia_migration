-module(m2k_export).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_logging.hrl").

-export([open_write/1,
         write/2,
         commit_write/1,
         abort_write/1]).

open_write(#{callback_mod := _,
             callback_mod_priv := _,
             table_copy_pid := _} = Args) ->
    State = Args#{step => log_header},
    {ok, State}.

write(#{step := log_header} = State, [LogHeader | Rest])
  when is_tuple(LogHeader) andalso element(1, LogHeader) =:= log_header ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] write: log_header",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    State1 = State#{step => schema},
    write(State1, Rest);
write(#{step := schema} = State, [Schema | Rest])
  when is_tuple(Schema) andalso element(1, Schema) =:= schema ->
    Table = element(2, Schema),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] write: init_copy/~ts",
       [Table],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    State1 = State#{step => {table, Table}},
    write(State1, Rest);
write(#{step := {table, Table},
        callback_mod := Mod,
        callback_mod_priv := ModPriv} = State, [Record | Rest]) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] write: record/~0p",
       [Record],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case Mod:copy_to_khepri(Table, Record, ModPriv) of
        {ok, ModPriv1} ->
            State1 = State#{callback_mod_priv => ModPriv1},
            write(State1, Rest);
        Error ->
            Error
    end;
write(State, []) ->
    {ok, State}.

commit_write(#{callback_mod_priv := ModPriv,
               table_copy_pid := TableCopyPid} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] commit_write",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    TableCopyPid ! {?MODULE, ModPriv},
    {ok, State}.

abort_write(#{callback_mod_priv := ModPriv,
              table_copy_pid := TableCopyPid} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] abort_write",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    TableCopyPid ! {?MODULE, ModPriv},
    {ok, State}.
