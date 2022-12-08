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

-record(?MODULE, {tables,
                  khepri_store,
                  callback_mod,
                  callback_mod_priv,
                  subscriber}).

proceed(SupPid) ->
    [{m2k_data_copy, DataCopyPid, _, _},
     {m2k_subscriber, SubscriberPid, _, _}] =
    lists:sort(supervisor:which_children(SupPid)),

    Ret = gen_server:call(
            DataCopyPid, {?FUNCTION_NAME, SubscriberPid}, infinity),
    case Ret of
        {exception, ?kmm_exception(_, _) = Exception} ->
            ?kmm_misuse(Exception);
        _ ->
            Ret
    end.

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%% -------------------------------------------------------------------
%% `gen_server' callbacks.
%% -------------------------------------------------------------------

init(#{khepri_store := StoreId,
       tables := Tables,
       callback_mod := Mod}) ->
    erlang:process_flag(trap_exit, true),
    State = #?MODULE{khepri_store = StoreId,
                     callback_mod = Mod,
                     tables = Tables},
    {ok, State}.

handle_call({proceed, SubscriberPid}, _From, State) ->
    State1 = State#?MODULE{subscriber = SubscriberPid},
    try
        State2 = do_copy_data(State1),
        {stop, normal, ok, State2}
    catch
        throw:?kmm_error(_, _) = Reason ->
            ?LOG_ERROR(
               "Failed to copy Mnesia->Khepri data: ~0p",
               [Reason],
               #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
            {stop, normal, {error, Reason}, State1};
        error:?kmm_exception(_, _) = Exception ->
            ?LOG_ERROR(
               "Exception during Mnesia->Khepri data copy: ~0p",
               [Exception],
               #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
            {stop, normal, {exception, Exception}, State1}
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

do_copy_data(State) ->
    ?LOG_INFO(
       "Copying data from Mnesia to Khepri",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),

    State1 = init_callback_mod(State),
    subscribe_to_mnesia_changes(State1),
    State2 = copy_from_mnesia_to_khepri(State1),
    State3 = final_sync_from_mnesia_to_khepri(State2),
    State4 = finish_callback_mod(State3),

    State4.

init_callback_mod(
  #?MODULE{tables = Tables,
           khepri_store = StoreId,
           callback_mod = Mod} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Initial callback mod ~s for Mnesia "
       "tables: ~0p",
       [Tables],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    case Mod:init_copy_to_khepri(Tables, StoreId) of
        {ok, ModPriv} ->
            State#?MODULE{callback_mod_priv = ModPriv};
        Error ->
            throw(
              ?kmm_error(
                 callback_mod_error,
                 #{callback_mod => Mod,
                   tables => Tables,
                   error => Error}))
    end.

subscribe_to_mnesia_changes(
  #?MODULE{tables = Tables, subscriber = SubscriberPid}) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Subscribe to Mnesia changes",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
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

copy_from_mnesia_to_khepri(
  #?MODULE{khepri_store = StoreId,
           tables = Tables,
           callback_mod = Mod,
           callback_mod_priv = ModPriv} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Start actual data copy",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    case mnesia:activate_checkpoint([{min, Tables}]) of
        {ok, Checkpoint, _Nodes} ->
            Args = #{khepri_store => StoreId,
                     callback_mod => Mod,
                     callback_mod_priv => ModPriv,
                     data_copy_pid => self()},
            Ret = mnesia:backup_checkpoint(Checkpoint, Args, m2k_export),
            _ = mnesia:deactivate_checkpoint(Checkpoint),
            ModPriv1 = receive
                           {m2k_export, MP} -> MP
                       after 0 ->
                                 ModPriv
                       end,
            case Ret of
                ok ->
                    State#?MODULE{callback_mod_priv = ModPriv1};
                Error ->
                    throw(
                      ?kmm_error(
                         callback_mod_error,
                         #{callback_mod => Mod,
                           error => Error}))
            end;
        Error ->
            throw(
              ?kmm_error(
                 failed_to_activate_mnesia_checkpoint,
                 #{tables => Tables,
                   error => Error}))
    end.

final_sync_from_mnesia_to_khepri(
  #?MODULE{subscriber = SubscriberPid,
          callback_mod = Mod,
          callback_mod_priv = ModPriv} = State) ->
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: Final sync",
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    case m2k_subscriber:flush(SubscriberPid, ModPriv) of
        {ok, ModPriv1} ->
            State#?MODULE{callback_mod_priv = ModPriv1};
        Error ->
            throw(
              ?kmm_error(
                 callback_mod_error,
                 #{callback_mod => Mod,
                   error => Error}))
    end.

finish_callback_mod(
  #?MODULE{callback_mod = Mod,
           callback_mod_priv = ModPriv} = State) ->
    Mod:finish_copy_to_khepri(ModPriv),
    State#?MODULE{callback_mod_priv = undefined}.
