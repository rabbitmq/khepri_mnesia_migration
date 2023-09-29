%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @private

-module(m2k_subscriber).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_error.hrl").
-include("src/kmm_logging.hrl").

-export([start_link/1,
         subscribe/2,
         drain/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(?MODULE, {converter_mod :: mnesia_to_khepri:converter_mod(),
                  subscribed_to = [] :: [mnesia_to_khepri:mnesia_table()],
                  events = []}).

subscribe(Pid, Tables) ->
    gen_server:call(Pid, {?FUNCTION_NAME, Tables}, infinity).

drain(Pid) ->
    gen_server:call(Pid, ?FUNCTION_NAME, infinity).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% -------------------------------------------------------------------
%% `gen_server' callbacks.
%% -------------------------------------------------------------------

init(#{converter_mod := Mod}) ->
    erlang:process_flag(trap_exit, true),
    State = #?MODULE{converter_mod = Mod},
    {ok, State}.

handle_call({subscribe, Tables}, _From, State) ->
    {Ret, State1} = do_subscribe(Tables, State),
    {reply, Ret, State1};
handle_call(drain, _From, #?MODULE{events = Events} = State) ->
    State1 = State#?MODULE{events = []},
    State2 = do_unsubscribe(State1),
    {reply, lists:reverse(Events), State2};
handle_call(Request, _From, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_call message: ~p",
       [Request],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {reply, undefined, State}.

handle_cast(Request, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_cast message: ~p",
       [Request],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {noreply, State}.

handle_info(
  {mnesia_table_event, {write, Table, NewRecord, _, _}},
  #?MODULE{events = Events} = State) ->
    Event = {put, Table, NewRecord},
    Events1 = [Event | Events],
    State1 = State#?MODULE{events = Events1},
    {noreply, State1};
handle_info(
  {mnesia_table_event, {delete, Table, {Table, Key}, _, _}},
  #?MODULE{events = Events} = State) ->
    Event = {delete, Table, Key},
    Events1 = [Event | Events],
    State1 = State#?MODULE{events = Events1},
    {noreply, State1};
handle_info(
  {mnesia_table_event, {delete, Table, Record, _, _}},
  #?MODULE{events = Events} = State) ->
    Key = element(2, Record),
    Event = {delete, Table, Key},
    Events1 = [Event | Events],
    State1 = State#?MODULE{events = Events1},
    {noreply, State1};
handle_info(Msg, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_info message: ~p",
       [Msg],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    {noreply, State}.

terminate(_Reason, State) ->
    _State1 = do_unsubscribe(State),
    ok.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

do_subscribe([Table | Rest], #?MODULE{subscribed_to = SubscribedTo} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: subscribe to changes to Mnesia table `~ts`",
       [Table],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),

    %% Ensure there is a local copy of the table we want to subscribe to.
    %% Otherwise, the subscribe call below will fail.
    _ = mnesia:add_table_copy(Table, node(), ram_copies),

    case mnesia:subscribe({table, Table, detailed}) of
        {ok, _} ->
            SubscribedTo1 = [Table | SubscribedTo],
            State1 = State#?MODULE{subscribed_to = SubscribedTo1},
            do_subscribe(Rest, State1);
        Error ->
            ?LOG_ERROR(
               "Mnesia->Khepri data copy: failed to subscribe to changes "
               "to Mnesia table `~ts`: ~p",
               [Table, Error],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            State1 = do_unsubscribe(State),
            {Error, State1}
    end;
do_subscribe([], State) ->
    {ok, State}.

do_unsubscribe(#?MODULE{subscribed_to = SubscribedTo} = State) ->
    do_unsubscribe1(SubscribedTo),
    State1 = State#?MODULE{subscribed_to = []},
    State1.

do_unsubscribe1([Table | Rest]) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: unsubscribe to changes to Mnesia table "
       "`~ts`",
       [Table],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case mnesia:unsubscribe({table, Table, detailed}) of
        {ok, _} ->
            do_unsubscribe1(Rest);
        Error ->
            ?LOG_WARNING(
               "Mnesia->Khepri data copy: failed to unsubscribe to changes "
               "to Mnesia table `~ts`: ~p",
               [Table, Error],
               #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
            do_unsubscribe1(Rest)
    end;
do_unsubscribe1([]) ->
    ok.
