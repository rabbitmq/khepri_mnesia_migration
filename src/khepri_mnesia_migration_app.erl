%%%-------------------------------------------------------------------
%% @doc khepri_mnesia_migration public API
%% @end
%%%-------------------------------------------------------------------

-module(khepri_mnesia_migration_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    khepri_mnesia_migration_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
