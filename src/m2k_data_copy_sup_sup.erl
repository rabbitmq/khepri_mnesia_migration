-module(m2k_data_copy_sup_sup).

-behaviour(supervisor).

-export([start_link/0,
         prepare_workers_sup/3]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

prepare_workers_sup(StoreId, Tables, Mod) ->
    supervisor:start_child(?SERVER, [#{khepri_store => StoreId,
                                       tables => Tables,
                                       callback_mod => Mod}]).

init([]) ->
    M2KDataCopySup = #{id => m2k_data_copy_sup,
                       restart => temporary,
                       start => {m2k_data_copy_sup, start_link, []}},

    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [M2KDataCopySup],
    {ok, {SupFlags, ChildSpecs}}.
