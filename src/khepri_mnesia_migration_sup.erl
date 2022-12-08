-module(khepri_mnesia_migration_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    M2KClusterSyncSup = #{id => m2k_cluster_sync_sup,
                          type => supervisor,
                          start => {m2k_cluster_sync_sup, start_link, []}},
    M2KTableCopySupSup = #{id => m2k_table_copy_sup_sup,
                           type => supervisor,
                           start => {m2k_table_copy_sup_sup, start_link, []}},

    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [M2KClusterSyncSup, M2KTableCopySupSup],
    {ok, {SupFlags, ChildSpecs}}.
