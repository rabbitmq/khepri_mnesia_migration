-module(m2k_subscriber).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_error.hrl").
-include("src/kmm_logging.hrl").

-export([start_link/1,
         subscribe/2,
         flush/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(?MODULE, {khepri_store,
                  callback_mod,
                  subscribed_to = [],
                  events = []}).

subscribe(Pid, Tables) ->
    gen_server:call(Pid, {?FUNCTION_NAME, Tables}, infinity).

flush(Pid) ->
    gen_server:call(Pid, ?FUNCTION_NAME, infinity).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% -------------------------------------------------------------------
%% `gen_server' callbacks.
%% -------------------------------------------------------------------

init(#{khepri_store := StoreId,
       callback_mod := Mod}) ->
    erlang:process_flag(trap_exit, true),
    State = #?MODULE{khepri_store = StoreId,
                     callback_mod = Mod},
    {ok, State}.

handle_call({subscribe, Tables}, _From, State) ->
    {Ret, State1} = do_subscribe(Tables, State),
    {reply, Ret, State1};
handle_call(flush, _From, State) ->
    try
        State1 = do_flush(State),
        {reply, ok, State1}
    catch
        throw:Error ->
            {reply, Error, State}
    end;
handle_call(Request, _From, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_call message: ~p",
       [Request],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    {reply, undefined, State}.

handle_cast(Request, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_cast message: ~p",
       [Request],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
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
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    {noreply, State}.

terminate(_Reason, State) ->
    _State1 = do_unsubscribe(State),
    ok.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

do_subscribe([Table | Rest], #?MODULE{subscribed_to = SubscribedTo} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Subscribe to changes to ~ts",
       [Table],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    case mnesia:subscribe({table, Table, detailed}) of
        {ok, _} ->
            SubscribedTo1 = [Table | SubscribedTo],
            State1 = State#?MODULE{subscribed_to = SubscribedTo1},
            do_subscribe(Rest, State1);
        Error ->
            ?LOG_ERROR(
               "Mnesia->Khepri data copy: Failed to subscribe to changes "
               "to ~ts",
               [Table],
               #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
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
       "Mnesia->Khepri data copy: Unsubscribe to changes to ~ts",
       [Table],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    case mnesia:unsubscribe({table, Table, detailed}) of
        {ok, _} ->
            do_unsubscribe1(Rest);
        Error ->
            ?LOG_WARNING(
               "Mnesia->Khepri data copy: Failed to subscribe to changes "
               "to ~ts: ~p",
               [Table, Error],
               #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
            do_unsubscribe1(Rest)
    end;
do_unsubscribe1([]) ->
    ok.

do_flush(#?MODULE{subscribed_to = SubscribedTo} = State) ->
    %% Switch all tables to read-only. All concurrent and future Mnesia
    %% transactions involving a write to one of them will fail with the
    %% `{no_exists, Table}' exception.
    make_tables_readonly(State),

    %% Unsubscribe to Mnesia events. All Mnesia tables are read-only at this
    %% point.
    State1 = do_unsubscribe(State),

    %% During the first round of copy, we received all write events as
    %% messages (parallel writes were authorized). Now, we want to consume
    %% those messages to record the writes we probably missed.
    State2 = consume_mnesia_events(SubscribedTo, State1),
    State2.

make_tables_readonly(#?MODULE{subscribed_to = SubscribedTo}) ->
    lists:foreach(
      fun(Table) ->
              ?LOG_DEBUG(
                 "Mnesia->Khepri data copy: Mark table ~ts as read-only",
                 [Table],
                 #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
              case mnesia:change_table_access_mode(Table, read_only) of
                  {atomic, ok}                              -> ok;
                  {aborted, {already_exists, _, read_only}} -> ok
              end
      end, SubscribedTo).

consume_mnesia_events(
  Tables,
  #?MODULE{khepri_store = StoreId,
           callback_mod = Mod,
           events = Events} = State) ->
    Events1 = lists:reverse(Events),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Consuming ~b Mnesia events from tables ~0p",
       [length(Events1), Tables],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    ModPrivs = lists:foldl(
                 fun(Table, MPs) ->
                         case Mod:init_copy_to_khepri(Table, StoreId) of
                             {ok, ModPriv} -> MPs#{Table => ModPriv};
                             Error         -> throw(Error)
                         end
                 end, #{}, Tables),
    consume_mnesia_events1(Events1, Mod, ModPrivs),
    State#?MODULE{events = []}.

consume_mnesia_events1([{put, Table, Record} | Rest], Mod, ModPrivs) ->
    ModPriv = maps:get(Table, ModPrivs),
    case Mod:copy_to_khepri(Record, ModPriv) of
        {ok, ModPriv1} ->
            ModPrivs1 = ModPrivs#{Table => ModPriv1},
            Remaining = length(Rest),
            if
                Remaining rem 100 =:= 0 ->
                    ?LOG_DEBUG(
                       "Mnesia->Khepri data copy: ~b Mnesia events left",
                       [Remaining],
                       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN});
                true ->
                    ok
            end,
            consume_mnesia_events1(Rest, Mod, ModPrivs1);
        Error ->
            throw(Error)
    end;
consume_mnesia_events1([{delete, Table, Key} | Rest], Mod, ModPrivs) ->
    ModPriv = maps:get(Table, ModPrivs),
    case Mod:delete_from_khepri(Key, ModPriv) of
        {ok, ModPriv1} ->
            ModPrivs1 = ModPrivs#{Table => ModPriv1},
            Remaining = length(Rest),
            if
                Remaining rem 100 =:= 0 ->
                    ?LOG_DEBUG(
                       "Mnesia->Khepri data copy: ~b Mnesia events left",
                       [Remaining],
                       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN});
                true ->
                    ok
            end,
            consume_mnesia_events1(Rest, Mod, ModPrivs1);
        Error ->
            throw(Error)
    end;
consume_mnesia_events1([], _Mod, _ModPrivs) ->
    ok.
