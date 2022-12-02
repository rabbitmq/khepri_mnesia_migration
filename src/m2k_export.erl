%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
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

open_write(#{converter_mod := _,
             converter_mod_priv := _,
             table_copy_pid := _} = Args) ->
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
    converter_mod := Mod,
    converter_mod_priv := ModPriv} = State,
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
    case Mod:copy_to_khepri(Table, Record1, ModPriv) of
        {ok, ModPriv1} ->
            State1 = State#{converter_mod_priv => ModPriv1},
            write(State1, Rest);
        Error ->
            Error
    end;
write(State, []) ->
    {ok, State}.

commit_write(#{converter_mod_priv := ModPriv,
               table_copy_pid := TableCopyPid} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] commit_write",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    TableCopyPid ! {?MODULE, ModPriv},
    {ok, State}.

abort_write(#{converter_mod_priv := ModPriv,
              table_copy_pid := TableCopyPid} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] abort_write",
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    TableCopyPid ! {?MODULE, ModPriv},
    {ok, State}.
