-module(m2k_data_copy).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_error.hrl").
-include("src/kmm_logging.hrl").

-export([start_link/1,
         proceed/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-record(?MODULE, {khepri_store,
                  callback_mod,
                  tables,
                  subscriber}).

proceed(SupPid) ->
    [{m2k_data_copy, DataCopyPid, _, _},
     {m2k_subscriber, SubscriberPid, _, _}] =
    lists:sort(supervisor:which_children(SupPid)),

    case gen_server:call(DataCopyPid, {?FUNCTION_NAME, SubscriberPid}) of
        {exception, ?kmm_exception(_, _) = Exception} ->
            ?kmm_misuse(Exception);
        Ret ->
            Ret
    end.

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% -------------------------------------------------------------------
%% `gen_server' callbacks.
%% -------------------------------------------------------------------

init(#{khepri_store := StoreId,
       callback_mod := Mod} = Args) ->
    erlang:process_flag(trap_exit, true),
    Tables = case Args of
                 #{tables := T} -> T;
                 _              -> lists:sort(mnesia:system_info(tables))
             end,
    State = #?MODULE{khepri_store = StoreId,
                     callback_mod = Mod,
                     tables = Tables},
    {ok, State}.

handle_call({proceed, SubscriberPid}, _From, State) ->
    State1 = State#?MODULE{subscriber = SubscriberPid},
    Ret = try
              do_copy_data(State1)
          catch
              throw:?kmm_error(_, _) = Reason ->
                  ?LOG_ERROR(
                     "Failed to copy Mnesia->Khepri data: ~0p",
                     [Reason],
                     #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
                  {error, Reason};
              error:?kmm_exception(_, _) = Exception ->
                  ?LOG_ERROR(
                     "Exception during Mnesia->Khepri data copy: ~0p",
                     [Exception],
                     #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
                  {exception, Exception}
          end,
    {reply, Ret, State1};
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

handle_info(Msg, State) ->
    ?LOG_WARNING(
       ?MODULE_STRING ": Unhandled handle_info message: ~p",
       [Msg],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

do_copy_data(#?MODULE{tables = Tables} = State) ->
    ?LOG_INFO(
       "Copying data from Mnesia to Khepri",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),

    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Copying from Mnesia tables: ~0p",
       [Tables],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),

    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Subscribe to Mnesia changes",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    ok = subscribe_to_mnesia_changes(State),

    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Actually copy data",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    ok = copy_from_mnesia_to_khepri(State),

    %% Mnesia transaction to handle received Mnesia events and tables removal.
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Final sync",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    ok = final_sync_from_mnesia_to_khepri(State),

    %% Unsubscribe to Mnesia events. All Mnesia tables are synchronized and
    %% read-only at this point.
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Unsubscribe to Mnesia changes",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    ok = unsubscribe_to_mnesia_changes(State).

subscribe_to_mnesia_changes(
  #?MODULE{tables = Tables, subscriber = SubscriberPid}) ->
    case m2k_subscriber:subscribe(SubscriberPid, Tables) of
        ok ->
            ok;
        Error ->
            throw(
              ?kmm_error(
                 failed_to_subscribe_to_mnesia_changes,
                 #{tables => Tables,
                   error => Error}))
    end.

copy_from_mnesia_to_khepri(#?MODULE{}) ->
    ok.

final_sync_from_mnesia_to_khepri(#?MODULE{}) ->
    ok.

unsubscribe_to_mnesia_changes(#?MODULE{subscriber = SubscriberPid}) ->
    m2k_subscriber:unsubscribe(SubscriberPid).
