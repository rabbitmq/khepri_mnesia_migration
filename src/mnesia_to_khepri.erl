-module(mnesia_to_khepri).

-export([sync_cluster_membership/0, sync_cluster_membership/1,
         copy_tables/2, copy_tables/3,
         copy_all_tables/1, copy_all_tables/2]).

sync_cluster_membership() ->
    StoreId = khepri_cluster:get_default_store_id(),
    sync_cluster_membership(StoreId).

sync_cluster_membership(StoreId) ->
    case m2k_cluster_sync_sup:prepare_worker(StoreId) of
        {ok, Pid} when is_pid(Pid) -> m2k_cluster_sync:proceed(Pid);
        {error, _} = Error         -> Error
    end.

copy_tables(Tables, Mod) ->
    StoreId = khepri_cluster:get_default_store_id(),
    copy_tables(StoreId, Tables, Mod).

copy_tables(StoreId, Tables, Mod) when is_list(Tables) andalso Tables =/= [] ->
    case m2k_table_copy_sup_sup:prepare_workers_sup(StoreId, Tables, Mod) of
        {ok, Pid} when is_pid(Pid) -> m2k_table_copy:proceed(Pid);
        {error, _} = Error         -> Error
    end.

copy_all_tables(Mod) ->
    Tables = list_all_tables(),
    copy_tables(Tables, Mod).

copy_all_tables(StoreId, Mod) ->
    Tables = list_all_tables(),
    copy_tables(StoreId, Tables, Mod).

list_all_tables() ->
    Tables0 = lists:sort(mnesia:system_info(tables)),
    Tables1 = Tables0 -- [schema],
    Tables1.
