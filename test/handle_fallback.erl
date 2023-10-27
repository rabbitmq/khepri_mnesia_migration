%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(handle_fallback).

-include_lib("eunit/include/eunit.hrl").

can_detect_infinite_loop_test() ->
    ok = mnesia:start(),
    Table = some_table,
    ?assertEqual(
       {aborted, {no_exists, Table}},
       mnesia_to_khepri:handle_fallback(
         %% No need for a store, `is_migration_finished()' will return false.
         non_existing_store,
         <<"id">>,
         fun() ->
                 mnesia:transaction(fun() -> mnesia:read(Table, some_key) end)
         end,
         ok)).
