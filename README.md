# `khepri_mnesia_migration`: Mnesia⬌Khepri migration toolkit

[![Hex.pm](https://img.shields.io/hexpm/v/khepri_mnesia_migration)](https://hex.pm/packages/khepri_mnesia_migration/)
[![Test](https://github.com/rabbitmq/khepri_mnesia_migration/actions/workflows/test.yaml/badge.svg)](https://github.com/rabbitmq/khepri_mnesia_migration/actions/workflows/test.yaml)
[![Codecov](https://codecov.io/gh/rabbitmq/khepri_mnesia_migration/branch/main/graph/badge.svg?token=R0OGKZ2RK2)](https://codecov.io/gh/rabbitmq/khepri_mnesia_migration)

`khepri_mnesia_migration` is a library providing helpers to migrate data
between the Mnesia and Khepri databases.

Currently, only the migration from Mnesia to Khepri is supported.

<img align="right" height="150" src="/doc/kmm-logo.svg">

## Project maturity

`khepri_mnesia_migration` is still under active development and should be
considered *Alpha* at this stage.

## Documentation

* A short tutorial in the [Getting started](#getting-started) section below
* [Documentation and API reference](https://rabbitmq.github.io/khepri_mnesia_migration/)

## Getting started

### Add as a dependency

Add `khepri_mnesia_migration` as a dependency of your project:

Using Rebar:

```erlang
%% In rebar.config
{deps, [{khepri_mnesia_migration, "0.6.0"}]}.
```

Using Erlang.mk:

```make
# In your Makefile
DEPS += khepri_mnesia_migration
dep_khepri_mnesia_migration = hex 0.6.0
```

Using Mix:

```elixir
# In mix.exs
defp deps do
  [
    {:khepri_mnesia_migration, "0.6.0"}
  ]
end
```

### Synchronize cluster members

To ensure a Khepri store has the same members as the Mnesia cluster, use
`mnesia_to_khepri:synchronize_cluster_membership/{0,1}`:

```erlang
mnesia_to_khepri:synchronize_cluster_membership(StoreId).
```

### Copy Mnesia tables to a Khepri store

You can copy Mnesia tables records to a Khepri store using
`mnesia_to_khepri:copy_tables/{2,3}`. It takes a converter module which takes
care of actually processing each Mnesia records (if needed) and storing them in
the Khepri store. A converter module called
`mnesia_to_khepri_example_converter` is provided for common use cases and as an
example.

```erlang
mnesia_to_khepri:copy_all_tables(mnesia_to_khepri_example_converter).
```

## How to build

### Build

```
rebar3 compile
```

### Build documentation

```
rebar3 edoc
```

### Test

```
rebar3 xref
rebar3 eunit
rebar3 ct --sname ct
rebar3 as test dialyzer
```

## Copyright and License

© 2022-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to
Broadcom Inc. and/or its subsidiaries.

This work is dual-licensed under the Apache License 2.0 and the Mozilla Public
License 2.0. You can choose between one of them if you use this work.

SPDX-License-Identifier: Apache-2.0 OR MPL-2.0
