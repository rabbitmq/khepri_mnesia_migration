%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Behavior defining converter modules used during Mnesia->Khepri copy.
%%
%% Unlike what the "optional callback functions" line says above, at least
%% one of {@link init_copy_to_khepri/3} or {@link init_copy_to_khepri/4} is
%% required, depending on how the copy is started.

-module(mnesia_to_khepri_converter).

-callback init_copy_to_khepri(StoreId, MigrationId, Tables) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, Priv} | {error, Reason},
      Priv :: any(),
      Reason :: any().

-callback init_copy_to_khepri(StoreId, MigrationId, Tables, Args) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Args :: any(),
      Ret :: {ok, Priv} | {error, Reason},
      Priv :: any(),
      Reason :: any().

-callback copy_to_khepri(Table, Record, Priv) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      Priv :: any(),
      Ret :: {ok, NewPriv} | {error, Reason},
      NewPriv :: any(),
      Reason :: any().

-callback delete_from_khepri(Table, Key, Priv) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      Priv :: any(),
      Ret :: {ok, NewPriv} | {error, Reason},
      NewPriv :: any(),
      Reason :: any().

-callback finish_copy_to_khepri(Priv) -> ok when
      Priv :: any().

-optional_callbacks([init_copy_to_khepri/3,
                     init_copy_to_khepri/4,
                     finish_copy_to_khepri/1]).

-export([init_copy_to_khepri/3, init_copy_to_khepri/4,
         copy_to_khepri/3,
         delete_from_khepri/3,
         finish_copy_to_khepri/1]).

-spec init_copy_to_khepri(StoreId, MigrationId, Tables) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Ret :: {ok, Priv},
      Priv :: priv.
%% @doc Initializes the state of the converter module.
%%
%% This callback is used when a table copy is initiated like this:
%% ```
%% mnesia_to_khepri:copy_all_tables(
%%   StoreId,
%%   <<"my_migration">>,
%%   my_converter_mod).
%% '''
%%
%% See {@link init_copy_to_khepri/4} if you want to pass arguments to the
%% converter module.
%%
%% It it called after involved tables were marked as "being migrated" (i.e.
%% copy in progress). This state is recorded in the destination Khepri store
%% `StoreId'.
%%
%% Unlike what the "optional callback functions" line says above, at least
%% one of {@link init_copy_to_khepri/3} or {@link init_copy_to_khepri/4} is
%% required, depending on how the copy is started.
%%
%% @see init_copy_to_khepri/4.

init_copy_to_khepri(_StoreId, _MigrationId, _Tables) ->
    {ok, priv}.

-spec  init_copy_to_khepri(StoreId, MigrationId, Tables, Args) -> Ret when
      StoreId :: khepri:store_id(),
      MigrationId :: mnesia_to_khepri:migration_id(),
      Tables :: [mnesia_to_khepri:mnesia_table()],
      Args :: any(),
      Ret :: {ok, Priv},
      Priv :: priv.
%% @doc Initializes the state of the converter module, using `Args'.
%%
%% This callback is used when a table copy is initiated like this:
%% ```
%% mnesia_to_khepri:copy_all_tables(
%%   StoreId,
%%   <<"my_migration">>,
%%   {my_converter_mod, Args}).
%% '''
%%
%% See {@link init_copy_to_khepri/3} if you don't need to pass arguments to
%% the converter module.
%%
%% It it called after involved tables were marked as being migrated. This
%% state is recorded in the destination Khepri store `StoreId'.
%%
%% Unlike what the "optional callback functions line says above, at least
%% one of {@link init_copy_to_khepri/3} or {@link init_copy_to_khepri/4} is
%% required, depending on how the copy is started.
%%
%% @see init_copy_to_khepri/3.

init_copy_to_khepri(_StoreId, _MigrationId, _Tables, _Args) ->
    {ok, priv}.

-spec copy_to_khepri(Table, Record, Priv) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Record :: tuple(),
      Priv :: priv,
      Ret :: {ok, Priv}.
%% @doc Copies a record to a Khepri store.
%%
%% This callback is called for each record to copy. `Record' comes from table
%% `Table' initially. It may not be called at all if there are not records to
%% copy.
%%
%% The callback is responsible for deciding if the record should be written to
%% Khepri and how. The destination Khepri store was passed to {@link
%% init_copy_to_khepri/2} or {@link init_copy_to_khepri/3}.

copy_to_khepri(_Table, _Record, Priv) ->
    {ok, Priv}.

-spec delete_from_khepri(Table, Key, Priv) -> Ret when
      Table :: mnesia_to_khepri:mnesia_table(),
      Key :: any(),
      Priv :: priv,
      Ret :: {ok, Priv}.
%% @doc Deletes a key from a Khepri store.
%%
%% This callback is called for each deletion which occurred in Mnesia
%% concurrently to the copy in progress. Only the Mnesia key is available, not
%% the record that was deleted. It may not be called at all if there were no
%% concurrent deletions.
%%
%% The callback is responsible for deciding what to do in Khepri. The
%% destination Khepri store was passed to {@link init_copy_to_khepri/3} or
%% {@link init_copy_to_khepri/4}.

delete_from_khepri(_Table, _Key, Priv) ->
    {ok, Priv}.

-spec finish_copy_to_khepri(Priv) -> ok when
      Priv :: priv.
%% @doc Performs any post-copy operations.
%%
%% It it called after involved tables were marked as "migrated" (i.e. copy is
%% finished). This state is recorded in the destination Khepri store
%% `StoreId'.
%%
%% This callback is optional.

finish_copy_to_khepri(_Priv) ->
    ok.
