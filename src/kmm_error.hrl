%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright © 2022-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
%%

-define(
   kmm_error(Name, Props),
   {khepri_mnesia_migration, Name, Props}).

-define(
   kmm_exception(Name, Props),
   {khepri_mnesia_migration_ex, Name, Props}).

-define(
   kmm_misuse(Exception),
   erlang:error(Exception)).

-define(
   kmm_misuse(Name, Props),
   ?kmm_misuse(?kmm_exception(Name, Props))).

-define(
   kmm_raise_misuse(Name, Props, Stacktrace),
   erlang:raise(error, ?kmm_exception(Name, Props), Stacktrace)).
