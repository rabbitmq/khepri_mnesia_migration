%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2024-2025 Broadcom. All Rights Reserved. The term "Broadcom"
%% refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(crashing_converter).

-behaviour(mnesia_to_khepri_converter).

-export([init_copy_to_khepri/3,
         copy_to_khepri/3,
         delete_from_khepri/3,
         finish_copy_to_khepri/1]).

-record(?MODULE, {store_id}).

init_copy_to_khepri(crash_during_init = StoreId, _MigrationId, _Tables) ->
    error({crash, StoreId});
init_copy_to_khepri(StoreId, _MigrationId, _Tables) ->
    State = #?MODULE{store_id = StoreId},
    {ok, State}.

copy_to_khepri(
  _Table, _Record,
  #?MODULE{store_id = crash_during_copy = StoreId}) ->
    error({crash, StoreId});
copy_to_khepri(
  _Table, _Record,
  State) ->
    {ok, State}.

delete_from_khepri(
  _Table, _Key,
  #?MODULE{store_id = crash_during_delete = StoreId}) ->
    error({crash, StoreId});
delete_from_khepri(
  _Table, _Key,
  State) ->
    {ok, State}.

finish_copy_to_khepri(#?MODULE{store_id = crash_during_finish = StoreId}) ->
    error({crash, StoreId});
finish_copy_to_khepri(_State) ->
    ok.
