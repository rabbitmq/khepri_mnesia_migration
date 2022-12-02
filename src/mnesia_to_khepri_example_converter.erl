%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Example converter module for use during Mnesia->Khepri copy.
%%
%% This converter module writes incoming Mnesia records to the Khepri store at
%% the `[Table, Key]' path. However, the `Key' MUST be an atom or a binary.

-module(mnesia_to_khepri_example_converter).

-behaviour(mnesia_to_khepri_converter).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_logging.hrl").

-export([init_copy_to_khepri/3,
         copy_to_khepri/3,
         delete_from_khepri/3]).

-record(?MODULE, {store_id :: khepri:store_id()}).

-spec init_copy_to_khepri(StoreId, MigrationId, Tables) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, Priv} | {error, Reason},
      Priv :: #?MODULE{},
      Reason :: any().
%% @private

init_copy_to_khepri(StoreId, _MigrationId, Tables) ->
    State = #?MODULE{store_id = StoreId},
    init_copy_to_khepri1(Tables, State).

init_copy_to_khepri1([Table | Rest], State) ->
    case mnesia:table_info(Table, type) of
        set ->
            init_copy_to_khepri1(Rest, State);
        Type ->
            {error, {?MODULE, mnesia_table_type_unsupported,
                     #{table => Table,
                       type => Type}}}
    end;
init_copy_to_khepri1([], State) ->
    {ok, State}.

-spec copy_to_khepri(Table, Record, Priv) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      Priv :: #?MODULE{},
      Ret :: {ok, NewPriv} | {error, Reason},
      NewPriv :: #?MODULE{},
      Reason :: any().
%% @private

copy_to_khepri(
  Table, Record,
  #?MODULE{store_id = StoreId} = State) ->
    Key = element(2, Record),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] key: ~0p",
       [Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Supported = is_atom(Key) orelse is_binary(Key),
    case Supported of
        true ->
            Path = [Table, Key],
            ?LOG_DEBUG(
               "Mnesia->Khepri data copy: [" ?MODULE_STRING "] path: ~0p",
               [Path],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            case khepri:put(StoreId, Path, Record) of
                ok    -> {ok, State};
                Error -> Error
            end;
        false ->
            {error, {?MODULE, mnesia_key_type_unsupported,
                     #{table => Table,
                       record => Record,
                       key => Key}}}
    end.

-spec delete_from_khepri(Table, Key, Priv) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      Priv :: #?MODULE{},
      Ret :: {ok, NewPriv} | {error, Reason},
      NewPriv :: #?MODULE{},
      Reason :: any().
%% @private

delete_from_khepri(
  Table, Key,
  #?MODULE{store_id = StoreId} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] key: ~0p",
       [Key],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    Supported = is_atom(Key) orelse is_binary(Key),
    case Supported of
        true ->
            Path = [Table, Key],
            ?LOG_DEBUG(
               "Mnesia->Khepri data copy: [" ?MODULE_STRING "] path: ~0p",
               [Path],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            case khepri:delete(StoreId, Path) of
                ok    -> {ok, State};
                Error -> Error
            end;
        false ->
            {error, {?MODULE, mnesia_key_type_unsupported,
                     #{table => Table,
                       key => Key}}}
    end.
