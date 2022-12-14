-module(m2k_cluster_sync_sup).

-behaviour(supervisor).

-export([start_link/0,
         prepare_worker/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

prepare_worker(StoreId) ->
    supervisor:start_child(?SERVER, [#{khepri_store => StoreId}]).

init([]) ->
    M2KClusterSync = #{id => m2k_cluster_sync,
                       restart => temporary,
                       start => {m2k_cluster_sync, start_link, []}},

    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [M2KClusterSync],
    {ok, {SupFlags, ChildSpecs}}.
