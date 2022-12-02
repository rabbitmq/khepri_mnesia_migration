%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private

-module(kmm_utils).

-include("src/kmm_error.hrl").

-export([is_mnesia_running/0,
         mnesia_nodes/0,
         %is_khepri_store_running/1,
         %khepri_nodes/1,
         erlang_node_uptime/0]).

is_mnesia_running() ->
    mnesia:system_info(is_running) =:= yes.

mnesia_nodes() ->
    case is_mnesia_running() of
        true ->
            AllNodes = lists:sort(mnesia:system_info(db_nodes)),
            RunningNodes = lists:sort(mnesia:system_info(running_db_nodes)),
            case AllNodes =:= RunningNodes of
                true ->
                    AllNodes;
                false ->
                    ?kmm_misuse(
                       all_mnesia_nodes_must_run,
                       #{all_nodes => AllNodes,
                         running_nodes => RunningNodes})
            end;
        false ->
            ?kmm_misuse(mnesia_must_run, #{node => node()})
    end.

%is_khepri_store_running(StoreId) ->
%    khepri_cluster:is_store_running(StoreId).
%
%khepri_nodes(StoreId) ->
%    case is_khepri_store_running(StoreId) of
%        true ->
%            AllNodes = lists:sort(khepri_cluster:nodes(StoreId)),
%            %% TODO: Ensure all nodes are running?
%            AllNodes;
%        false ->
%            ?kmm_misuse(
%               khepri_store_must_run,
%               #{node => node(),
%                 store_id => StoreId})
%    end.

erlang_node_uptime() ->
    CurrentTime = erlang:monotonic_time(),
    StartTime = erlang:system_info(start_time),
    erlang:convert_time_unit(CurrentTime - StartTime, native, millisecond).
