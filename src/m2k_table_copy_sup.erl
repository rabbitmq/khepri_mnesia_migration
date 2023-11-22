%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

%% @private

-module(m2k_table_copy_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Args) ->
    supervisor:start_link(?MODULE, [Args]).

init([Args]) ->
    M2KSubscriber = #{id => m2k_subscriber,
                      restart => transient,
                      significant => true,
                      start => {m2k_subscriber, start_link, [Args]}},
    M2KTableCopy = #{id => m2k_table_copy,
                     restart => transient,
                     significant => true,
                     start => {m2k_table_copy, start_link, [Args]}},

    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1,
                 auto_shutdown => any_significant},
    ChildSpecs = [M2KSubscriber, M2KTableCopy],
    {ok, {SupFlags, ChildSpecs}}.
