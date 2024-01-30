%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @private

-module(m2k_export).

-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/logger.hrl").

-include("src/kmm_logging.hrl").

-export([open_write/1,
         write/2,
         commit_write/1,
         abort_write/1]).

open_write(#{table_copy_pid := _} = Args) ->
    State = Args#{table_to_record_name => #{}},
    {ok, State}.

write(
  State,
  [LogHeader | Rest])
  when is_tuple(LogHeader) andalso
       element(1, LogHeader) =:= log_header andalso
       not is_map_key(table, State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] write: log_header",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    write(State, Rest);
write(
  #{table_to_record_name := TableToRecordName} = State,
  [{schema, Table, TableInfo} | Rest]) ->
    State1 = case proplists:is_defined(record_name, TableInfo) of
                 false ->
                     ?LOG_DEBUG(
                        "Mnesia->Khepri data copy: [" ?MODULE_STRING "] "
                        "write: init_copy, table=~ts",
                        [Table],
                        #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                     State;
                 true ->
                     RecordName = proplists:get_value(record_name, TableInfo),
                     ?LOG_DEBUG(
                        "Mnesia->Khepri data copy: [" ?MODULE_STRING "] "
                        "write: init_copy, table=~ts, record=~ts",
                        [Table, RecordName],
                        #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                     TableToRecordName1 = TableToRecordName#{
                                            Table => RecordName},
                     State#{table_to_record_name => TableToRecordName1}
             end,
    write(State1, Rest);
write(
  #{table_to_record_name := TableToRecordName,
    table_copy_pid := TableCopyPid} = State,
  [Record | Rest]) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] write: record/~0p",
       [Record],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Table = element(1, Record),
    Record1 = case TableToRecordName of
                  #{Table := RecordName} -> setelement(1, Record, RecordName);
                  _                      -> Record
              end,
    TableCopyPid ! {?MODULE, self(), handle_record, Table, Record1},
    Result = receive
                 {TableCopyPid, record_handled, Res} ->
                     Res
             after
                 15_000 ->
                     ?LOG_ERROR(
                        "Mnesia->Khepri data copy: [" ?MODULE_STRING "] timeout: record/~0p",
                        [Record],
                        #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
                     {error, timeout}
             end,
    case Result of
        ok ->
            write(State, Rest);
        Error ->
            Error
    end;
write(State, []) ->
    {ok, State}.

commit_write(State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] commit_write",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {ok, State}.

abort_write(State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] abort_write",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {ok, State}.
