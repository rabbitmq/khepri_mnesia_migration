-module(mnesia_to_khepri).

-export([sync_cluster_membership/0, sync_cluster_membership/1,
         copy_data/1, copy_data/2]).

sync_cluster_membership() ->
    StoreId = khepri_cluster:get_default_store_id(),
    sync_cluster_membership(StoreId).

sync_cluster_membership(StoreId) ->
    case m2k_cluster_sync_sup:prepare_worker(StoreId) of
        {ok, Pid} when is_pid(Pid) -> m2k_cluster_sync:proceed(Pid);
        {error, _} = Error         -> Error
    end.

copy_data(Mod) ->
    StoreId = khepri_cluster:get_default_store_id(),
    copy_data(StoreId, Mod).

copy_data(StoreId, Mod) ->
    case m2k_data_copy_sup_sup:prepare_workers_sup(StoreId, Mod) of
        {ok, Pid} when is_pid(Pid) -> m2k_data_copy:proceed(Pid);
        {error, _} = Error         -> Error
    end.
