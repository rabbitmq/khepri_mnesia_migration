-module(mnesia_to_khepri).

-export([sync_cluster/0, sync_cluster/1]).

sync_cluster() ->
    StoreId = khepri_cluster:get_default_store_id(),
    sync_cluster(StoreId).

sync_cluster(StoreId) ->
    case m2k_cluster_sync_sup:prepare_cluster_sync_worker(StoreId) of
        {ok, Pid} when is_pid(Pid) -> m2k_cluster_sync:proceed(Pid);
        {error, _} = Error         -> Error
    end.
