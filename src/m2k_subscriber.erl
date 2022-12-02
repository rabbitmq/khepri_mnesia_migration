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
         flush/2,
         make_tables_readwrite/1]).
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

flush(Pid, ModPriv) ->
    gen_server:call(Pid, {?FUNCTION_NAME, ModPriv}, infinity).

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
handle_call({flush, ModPriv}, _From, State) ->
    try
        {ModPriv1, State1} = do_flush(ModPriv, State),
        {reply, {ok, ModPriv1}, State1}
    catch
        throw:Error ->
            {reply, Error, State}
    end;
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

do_flush(ModPriv, #?MODULE{subscribed_to = SubscribedTo} = State) ->
    %% Switch all tables to read-only. All concurrent and future Mnesia
    %% transactions involving a write to one of them will fail with the
    %% `{no_exists, Table}' exception.
    make_tables_readonly(SubscribedTo),

    try
        %% Unsubscribe to Mnesia events. All Mnesia tables are read-only at
        %% this point.
        State1 = do_unsubscribe(State),

        %% During the first round of copy, we received all write events as
        %% messages (parallel writes were authorized). Now, we want to consume
        %% those messages to record the writes we probably missed.
        {ModPriv1, State2} = consume_mnesia_events(
                               SubscribedTo, ModPriv, State1),
        {ModPriv1, State2}
    catch
        Class:Reason:Stacktrace ->
            make_tables_readwrite(SubscribedTo),
            erlang:raise(Class, Reason, Stacktrace)
    end.

make_tables_readonly(Tables) ->
    make_tables_readonly(Tables, []).

make_tables_readonly([Table | Rest], AlreadyMarked) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: mark Mnesia table `~ts` as read-only",
       [Table],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    case mnesia:change_table_access_mode(Table, read_only) of
        {atomic, ok} ->
            make_tables_readonly(Rest, [Table | AlreadyMarked]);
        {aborted, {already_exists, _, read_only}} ->
            make_tables_readonly(Rest, [Table | AlreadyMarked]);
        {aborted, _} = Error ->
            _ = make_tables_readwrite(AlreadyMarked),
            throw(Error)
    end;
make_tables_readonly([], _AlreadyMarked) ->
    ok.

make_tables_readwrite([Table | Rest]) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: mark Mnesia table `~ts` as read-write after "
       "a failed copy or a rollback",
       [Table],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    _ = mnesia:change_table_access_mode(Table, read_write),
    make_tables_readwrite(Rest);
make_tables_readwrite([]) ->
    ok.

consume_mnesia_events(
  Tables,
  ModPriv,
  #?MODULE{converter_mod = Mod,
           events = Events} = State) ->
    Events1 = lists:reverse(Events),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: consuming ~b Mnesia events from tables ~0p",
       [length(Events1), Tables],
       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN}),
    ModPriv1 = consume_mnesia_events1(Events1, Mod, ModPriv),
    State1 = State#?MODULE{events = []},
    {ModPriv1, State1}.

consume_mnesia_events1([{put, Table, Record} | Rest], Mod, ModPriv) ->
    case Mod:copy_to_khepri(Table, Record, ModPriv) of
        {ok, ModPriv1} ->
            Remaining = length(Rest),
            if
                Remaining rem 100 =:= 0 ->
                    ?LOG_DEBUG(
                       "Mnesia->Khepri data copy: ~b Mnesia events left",
                       [Remaining],
                       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN});
                true ->
                    ok
            end,
            consume_mnesia_events1(Rest, Mod, ModPriv1);
        Error ->
            throw(Error)
    end;
consume_mnesia_events1([{delete, Table, Key} | Rest], Mod, ModPriv) ->
    case Mod:delete_from_khepri(Table, Key, ModPriv) of
        {ok, ModPriv1} ->
            Remaining = length(Rest),
            if
                Remaining rem 100 =:= 0 ->
                    ?LOG_DEBUG(
                       "Mnesia->Khepri data copy: ~b Mnesia events left",
                       [Remaining],
                       #{domain => ?KMM_M2K_TABLE_COPY_LOG_DOMAIN});
                true ->
                    ok
            end,
            consume_mnesia_events1(Rest, Mod, ModPriv1);
        Error ->
            throw(Error)
    end;
consume_mnesia_events1([], _Mod, ModPriv) ->
    ModPriv.
