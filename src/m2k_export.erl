-module(m2k_export).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_logging.hrl").

-export([open_write/1,
         write/2,
         commit_write/1,
         abort_write/1]).

open_write(Args) ->
    State = Args#{step => log_header},
    {ok, State}.

write(#{step := log_header} = State, [LogHeader | Rest])
  when is_tuple(LogHeader) andalso element(1, LogHeader) =:= log_header ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] write: log_header",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    State1 = State#{step => schema},
    write(State1, Rest);
write(#{step := schema,
        khepri_store := StoreId,
        callback_mod := Mod} = State, [Schema | Rest])
  when is_tuple(Schema) andalso element(1, Schema) =:= schema ->
    State1 = finish_copy(State),
    Table = element(2, Schema),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] write: init_copy/~ts",
       [Table],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    case Mod:init_copy_to_khepri(Table, StoreId) of
        {ok, ModPriv} ->
            State2 = State1#{step => {table, Table},
                             callback_mod_priv => ModPriv},
            write(State2, Rest);
        Error ->
            Error
    end;
write(#{step := {table, _Table},
        callback_mod := Mod,
        callback_mod_priv := ModPriv} = State, [Record | Rest]) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] write: record/~0p",
       [Record],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    case Mod:copy_to_khepri(Record, ModPriv) of
        {ok, ModPriv1} ->
            State1 = State#{callback_mod_priv => ModPriv1},
            write(State1, Rest);
        Error ->
            Error
    end;
write(State, []) ->
    {ok, State}.

commit_write(State) ->
    State1 = finish_copy(State),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] commit_write",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    {ok, State1}.

abort_write(State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] abort_write",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    State1 = finish_copy(State),
    {ok, State1}.

finish_copy(#{step := {table, Table},
              callback_mod := Mod,
              callback_mod_priv := ModPriv} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] write: finish_copy/~ts",
       [Table],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    Mod:finish_copy_to_khepri(ModPriv),
    maps:remove(callback_mod_priv, State);
finish_copy(State) ->
    State.
