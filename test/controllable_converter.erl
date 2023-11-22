%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(controllable_converter).

-behaviour(mnesia_to_khepri_converter).

-export([init_copy_to_khepri/4,
         copy_to_khepri/3,
         delete_from_khepri/3]).

-record(?MODULE, {store_id,
                  caller}).

init_copy_to_khepri(StoreId, _MigrationId, _Tables, Caller) ->
    State = #?MODULE{store_id = StoreId,
                     caller = Caller},
    Caller ! {?FUNCTION_NAME, StoreId, self()},
    receive proceed -> ok end,
    {ok, State}.

copy_to_khepri(_Table, _Record, State) ->
    {ok, State}.

delete_from_khepri(_Table, _Key, State) ->
    {ok, State}.
