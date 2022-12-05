-module(m2k_subscriber).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_error.hrl").
-include("src/kmm_logging.hrl").

-export([start_link/1,
         subscribe/2,
         unsubscribe/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(?MODULE, {khepri_store,
                  callback_mod,
                  subscribed_to = []}).

subscribe(Pid, Tables) ->
    gen_server:call(Pid, {?FUNCTION_NAME, Tables}).

unsubscribe(Pid) ->
    gen_server:cast(Pid, ?FUNCTION_NAME).

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
handle_call(Request, _From, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_call message: ~p",
       [Request],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    {reply, undefined, State}.

handle_cast(unsubscribe, #?MODULE{subscribed_to = SubscribedTo} = State) ->
    do_unsubscribe(SubscribedTo),
    State1 = State#?MODULE{subscribed_to = []},
    {noreply, State1};
handle_cast(Request, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_cast message: ~p",
       [Request],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    {noreply, State}.

handle_info(Msg, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_info message: ~p",
       [Msg],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    {noreply, State}.

terminate(_Reason, #?MODULE{subscribed_to = SubscribedTo} = _State) ->
    do_unsubscribe(SubscribedTo),
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
            do_unsubscribe(SubscribedTo),
            State1 = State#?MODULE{subscribed_to = []},
            {Error, State1}
    end;
do_subscribe([], State) ->
    {ok, State}.

do_unsubscribe([Table | Rest]) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Unsubscribe to changes to ~ts",
       [Table],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    case mnesia:unsubscribe({table, Table, detailed}) of
        {ok, _} ->
            do_unsubscribe(Rest);
        Error ->
            ?LOG_WARNING(
               "Mnesia->Khepri data copy: Failed to subscribe to changes "
               "to ~ts: ~p",
               [Table, Error],
               #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
            do_unsubscribe(Rest)
    end;
do_unsubscribe([]) ->
    ok.
