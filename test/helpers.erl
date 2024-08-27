%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(helpers).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-export([init_list_of_modules_to_skip/0,
         start_erlang_node/1,
         stop_erlang_node/2,
         start_ra_system/1,
         reset_ra_system/1,
         stop_ra_system/1,
         store_dir_name/1,
         remove_store_dir/1,
         cluster_mnesia_nodes/1,
         mnesia_cluster_members/1,
         khepri_cluster_members/2,
         setup_node/1,
         basic_logger_config/0,
         start_epmd/0,
         start_n_nodes/3,
         stop_nodes/1,
         with_log/1,
         capture_log/1,
         silence_default_logger/0,
         restore_default_logger/1,
         %% For internal use only.
         log/2,
         format/2]).

-define(CAPTURE_LOGGER_ID, capture_logger).

init_list_of_modules_to_skip() ->
    _ = application:load(khepri),
    khepri_utils:init_list_of_modules_to_skip().

start_ra_system(RaSystem) when is_atom(RaSystem) ->
    StoreDir = store_dir_name(RaSystem),
    Props = #{ra_system => RaSystem,
              store_dir => StoreDir},
    start_ra_system(Props);
start_ra_system(#{ra_system := RaSystem, store_dir := StoreDir} = Props) ->
    _ = remove_store_dir(StoreDir),
    {ok, _} = application:ensure_all_started(ra),
    Default = ra_system:default_config(),
    RaSystemConfig = Default#{name => RaSystem,
                              data_dir => StoreDir,
                              wal_data_dir => StoreDir,
                              wal_max_size_bytes => 16 * 1024,
                              names => ra_system:derive_names(RaSystem)},
    case ra_system:start(RaSystemConfig) of
        {ok, RaSystemPid} ->
            Props#{ra_system_pid => RaSystemPid};
        {error, _} = Error ->
            throw(Error)
    end.

reset_ra_system(Props) ->
    stop_ra_system(Props),
    start_ra_system(Props).

stop_ra_system(#{ra_system := RaSystem,
                 store_dir := StoreDir}) ->
    ?assertEqual(ok, supervisor:terminate_child(ra_systems_sup, RaSystem)),
    ?assertEqual(ok, supervisor:delete_child(ra_systems_sup, RaSystem)),
    _ = remove_store_dir(StoreDir),
    ok.

store_dir_name(RaSystem) ->
    Node = node(),
    lists:flatten(
      io_lib:format("_test.khepri.~s.~s", [RaSystem, Node])).

remove_store_dir(StoreDir) ->
    OnWindows = case os:type() of
                    {win32, _} -> true;
                    _          -> false
                end,
    case file:del_dir_r(StoreDir) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, eexist} when OnWindows ->
            %% FIXME: Some files are not deleted on Windows... Are they still
            %% open in Ra?
            io:format(
              standard_error,
              "Files remaining in ~ts: ~p~n",
              [StoreDir, file:list_dir_all(StoreDir)]),
            ok;
        Error ->
            throw(Error)
    end.

cluster_mnesia_nodes([FirstNode | _OtherNodes] = Nodes) ->
    ct:pal("Create Mnesia cluster"),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(
                 stopped,
                 rpc:call(Node, mnesia, stop, [])),
              ?assertEqual(
                 ok,
                 rpc:call(Node, mnesia, delete_schema, [[Node]]))
      end, Nodes),

    ?assertMatch(
       ok,
       rpc:call(FirstNode, mnesia, create_schema, [Nodes])),

    lists:foreach(
      fun(Node) ->
              ?assertEqual(ok, rpc:call(Node, mnesia, start, []))
      end, Nodes),

    ?assertEqual(
       lists:sort(Nodes),
       mnesia_cluster_members(FirstNode)).

mnesia_cluster_members(Node) ->
    Nodes = rpc:call(Node, mnesia, system_info, [db_nodes]),
    lists:sort(Nodes).

khepri_cluster_members(Node, StoreId) ->
    {ok, Nodes} = rpc:call(Node, khepri_cluster, nodes, [StoreId]),
    lists:sort(Nodes).

-define(LOGFMT_CONFIG, #{legacy_header => false,
                         single_line => false,
                         template => [time, " ", pid, ": ", msg, "\n"]}).

setup_node(PrivDir) ->
    basic_logger_config(),

    %% We use an additional logger handler for messages tagged with a non-OTP
    %% domain because by default, `cth_log_redirect' drops them.
    GL = erlang:group_leader(),
    GLNode = node(GL),
    Ret = logger:add_handler(
            cth_log_redirect_any_domains, cth_log_redirect_any_domains,
            #{config => #{group_leader => GL,
                          group_leader_node => GLNode}}),
    case Ret of
        ok                          -> ok;
        {error, {already_exist, _}} -> ok
    end,
    ok = logger:set_handler_config(
           cth_log_redirect_any_domains, formatter,
           {logger_formatter, ?LOGFMT_CONFIG}),
    ?LOG_INFO(
       "Extended logger configuration (~s):~n~p",
       [node(), logger:get_config()]),

    Node = node(),
    MnesiaBasename = lists:flatten(
                       io_lib:format("_test.mnesia.~s", [Node])),
    MnesiaDir = filename:join(PrivDir, MnesiaBasename),
    ok = application:set_env(
           mnesia, dir, MnesiaDir, [{persistent, true}]),
    ok = mnesia:create_schema([Node]),

    ok = application:set_env(
           khepri, default_timeout, 5000, [{persistent, true}]),

    ok.

basic_logger_config() ->
    _ = logger:set_primary_config(level, debug),

    HandlerIds = [HandlerId ||
                  HandlerId <- logger:get_handler_ids(),
                  HandlerId =:= default orelse
                  HandlerId =:= cth_log_redirect],
    lists:foreach(
      fun(HandlerId) ->
              ok = logger:set_handler_config(
                    HandlerId, formatter,
                    {logger_formatter, ?LOGFMT_CONFIG}),
              _ = logger:add_handler_filter(
                    HandlerId, progress,
                    {fun logger_filters:progress/2,stop}),
              _ = logger:remove_handler_filter(
                    HandlerId, remote_gl)
      end, HandlerIds),
    ?LOG_INFO(
       "Basic logger configuration (~s):~n~p",
       [node(), logger:get_config()]),

    ok.

start_epmd() ->
    RootDir = code:root_dir(),
    ErtsVersion = erlang:system_info(version),
    ErtsDir = lists:flatten(io_lib:format("erts-~ts", [ErtsVersion])),
    EpmdPath0 = filename:join([RootDir, ErtsDir, "bin", "epmd"]),
    EpmdPath = case os:type() of
                   {win32, _} -> EpmdPath0 ++ ".exe";
                   _          -> EpmdPath0
               end,
    Port = erlang:open_port(
             {spawn_executable, EpmdPath},
             [{args, ["-daemon"]}]),
    erlang:port_close(Port),
    ok.

start_n_nodes(Config, NamePrefix, Count) ->
    ct:pal("Start ~b Erlang nodes:", [Count]),
    Nodes = [begin
                 Name = lists:flatten(
                          io_lib:format(
                            "~s-~s-~b", [?MODULE, NamePrefix, I])),
                 ct:pal("- ~s", [Name]),
                 start_erlang_node(Name)
             end || I <- lists:seq(1, Count)],
    ct:pal("Started nodes: ~p", [[Node || {Node, _Peer} <- Nodes]]),

    %% We add all nodes to the test coverage report.
    CoveredNodes = [Node || {Node, _Peer} <- Nodes],
    {ok, _} = cover:start([node() | CoveredNodes]),

    CodePath = code:get_path(),
    PrivDir = ?config(priv_dir, Config),
    lists:foreach(
      fun({Node, _Peer}) ->
              rpc:call(Node, code, add_pathsz, [CodePath]),
              ok = rpc:call(Node, ?MODULE, setup_node, [PrivDir])
      end, Nodes),
    Nodes.

stop_nodes(Nodes) ->
    lists:foreach(
      fun({Node, Peer}) ->
              ok = cover:stop(Node),
              stop_erlang_node(Node, Peer)
      end,
      Nodes).

-if(?OTP_RELEASE >= 25).
start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    {ok, Peer, Node} = peer:start(#{name => Name1,
                                    wait_boot => infinity}),
    {Node, Peer}.

stop_erlang_node(_Node, Peer) ->
    ok = peer:stop(Peer).
-else.
start_erlang_node(Name) ->
    Name1 = list_to_atom(Name),
    Options = [{monitor_master, true}],
    {ok, Node} = ct_slave:start(Name1, Options),
    {Node, Node}.

stop_erlang_node(_Node, Node) ->
    {ok, _} = ct_slave:stop(Node),
    ok.
-endif.

silence_default_logger() ->
    {ok, #{level := OldDefaultLoggerLevel}} =
      logger:get_handler_config(default),
    ok = logger:set_handler_config(default, level, none),
    OldDefaultLoggerLevel.

restore_default_logger(OldDefaultLoggerLevel) ->
    ok = logger:set_handler_config(default, level, OldDefaultLoggerLevel).

-spec with_log(Fun) -> {Result, Log}
    when
      Fun :: fun(() -> Result),
      Result :: term(),
      Log :: binary().

%% @doc Returns the value of executing the given `Fun' and any log messages
%% produced while executing it, concatenated into a binary.
with_log(Fun) ->
    FormatterConfig = #{},
    HandlerConfig = #{config => self(),
                      formatter => {?MODULE, FormatterConfig}},
    {ok, #{level := OldDefaultLogLevel}} = logger:get_handler_config(default),
    ok = logger:set_handler_config(default, level, none),
    ok = logger:add_handler(?CAPTURE_LOGGER_ID, ?MODULE, HandlerConfig),
    try
        Result = Fun(),
        Log = collect_logs(),
        {Result, Log}
    after
        _ = logger:remove_handler(?CAPTURE_LOGGER_ID),
        _ = logger:set_handler_config(default, level, OldDefaultLogLevel)
    end.

-spec capture_log(Fun) -> Log
    when
      Fun :: fun(() -> any()),
      Log :: binary().

%% @doc Returns the logger messages produced while executing the given `Fun'
%% concatenated into a binary.
capture_log(Fun) ->
    {_Result, Log} = with_log(Fun),
    Log.

%% Implements the `log/2' callback for logger handlers
log(LogEvent, Config) ->
    #{config := TestPid} = Config,
    Msg = case maps:get(msg, LogEvent) of
              {report, Report} ->
                  {Format, Args} = logger:format_report(Report),
                  iolist_to_binary(io_lib:format(Format, Args));
              {string, Chardata} ->
                  unicode:characters_to_binary(Chardata);
              {Format, Args} ->
                  iolist_to_binary(io_lib:format(Format, Args))
          end,
    TestPid ! {?MODULE, Msg},
    ok.

%% Implements the `format/2' callback for logger formatters
format(_LogEvent, _FormatConfig) ->
    %% No-op: print nothing to the console.
    ok.

collect_logs() ->
    collect_logs(<<>>).

collect_logs(Acc) ->
    receive
        {?MODULE, Msg} -> collect_logs(<<Msg/binary, Acc/binary>>)
    after
        50 -> Acc
    end.
