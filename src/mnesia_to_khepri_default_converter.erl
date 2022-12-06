-module(mnesia_to_khepri_default_converter).

-include_lib("kernel/include/logger.hrl").

-include("src/kmm_logging.hrl").

-export([init_copy_to_khepri/2,
         copy_to_khepri/2,
         finish_copy_to_khepri/1]).

-record(?MODULE, {table,
                  store_id,
                  index_positions}).

init_copy_to_khepri(Table, StoreId) ->
    case mnesia:table_info(Table, type) of
        set ->
            State = #?MODULE{table = Table,
                             store_id = StoreId},
            {ok, State};
        Type ->
            {error, {?MODULE, mnesia_table_type_unsupported,
                     #{table => Table,
                       type => Type}}}
    end.

copy_to_khepri(
  Record,
  #?MODULE{table = Table,
           store_id = StoreId} = State) ->
    Key = element(2, Record),
    ?LOG_DEBUG(
       "Mnesia->Khepri data copy: [" ?MODULE_STRING "] key: ~0p",
       [Key],
       #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
    Supported = is_atom(Key) orelse is_binary(Key),
    case Supported of
        true ->
            Path = [Table, Key],
            ?LOG_DEBUG(
               "Mnesia->Khepri data copy: [" ?MODULE_STRING "] path: ~0p",
               [Path],
               #{domain => ?KMM_M2K_DATA_COPY_LOG_DOMAIN}),
            case khepri:put(StoreId, Path, Record) of
                ok    -> {ok, State};
                Error -> Error
            end;
        false ->
            {error, {?MODULE, mnesia_key_type_unsupported,
                     #{table => Table,
                       record => Record,
                       key => Key}}}
    end.

finish_copy_to_khepri(_State) ->
    ok.
