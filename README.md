InfluxDB Proxy
======

This project adds a basic high availability layer to InfluxDB.

NOTE: influx-proxy must be built with Go 1.7+, don't implement udp.

Why
---

We used [InfluxDB Relay](https://github.com/influxdata/influxdb-relay) before, but it doesn't support some demands.
We use grafana for visualizing time series data, so we need add datasource for grafana. We need change the datasource config when influxdb is down.
We need transfer data across idc, but Relay doesn't support gzip.
It's inconvenient to analyse data with connecting different influxdb.
Therefore, we made InfluxDB Proxy.

Features
--------

* Support gzip.
* Support query.
* Filter some dangerous influxql.
* Transparent for client, like cluster for client.
* Cache data to file when write failed, then rewrite.

Requirements
-----------

* Golang >= 1.7

Usage
------------

#### Quickstart

```sh
$ # Install influx-proxy to your $GOPATH/bin
$ go get -u github.com/chengshiwen/influx-proxy/service
$ mv $GOPATH/bin/service $GOPATH/bin/influx-proxy
$ # Start influx-proxy!
$ $GOPATH/bin/influx-proxy -config proxy.json
```

#### Build Release

```sh
$ cd $GOPATH/src/github.com/chengshiwen/influx-proxy
$ # build current platform
$ make build
$ # build linux amd64
$ make linux
```

Description
-----------

The architecture is fairly simple, one InfluxDB Proxy process and two or more InfluxDB processes. The Proxy should point HTTP requests with measurements to the two InfluxDB servers.

The setup should look like this:

```
        ┌─────────────────┐
        │writes & queries │
        └─────────────────┘
                 │
                 ▼
         ┌───────────────┐
         │               │
         │InfluxDB Proxy │
         |  (only http)  |
         │               │
         └───────────────┘
                 │
                 ▼
        ┌─────────────────┐
        │   measurements  │
        └─────────────────┘
          |              |
        ┌─┼──────────────┘
        │ └──────────────┐
        ▼                ▼
  ┌──────────┐      ┌──────────┐
  │          │      │          │
  │ InfluxDB │      │ InfluxDB │
  │          │      │          │
  └──────────┘      └──────────┘
```

Measurements match principle:

* **Exact match first**. For instance, we use `cpu.load` for measurement's name. The KEYMAPS has `cpu` and `cpu.load` keys.
It will use the `cpu.load` corresponding backends.

* **Prefix match then**. For instance, we use `cpu.load` for measurement's name. The KEYMAPS only has `cpu` key.
It will use the `cpu` corresponding backends.

* **\_default\_ match finally**. For instance, we use `cpu.load` for measurement's name. The KEYMAPS only has `_default_` key.
It will use the `_default_` corresponding backends.

Proxy Configuration
--------

The configurations in `proxy.json` are the following:

#### BACKENDS

* `url`: influxdb addr or other http backend which supports influxdb line protocol
* `db`: influxdb db
* `username`: influxdb username
* `password`: influxdb password
* `interval`: default config is 1000ms, wait 1 second write whether point count has bigger than maxrowlimit config
* `timeout`: default config is 10000ms, write timeout until 10 seconds
* `timeoutquery`: default config is 600000ms, query timeout until 600 seconds
* `maxrowlimit`: default config is 10000, wait 10000 points write
* `checkinterval`: default config is 1000ms, check backend active every 1 second
* `rewriteinterval`: default config is 10000ms, rewrite every 10 seconds
* `writeonly`: default 0

#### KEYMAPS

* `measurement: [BACKENDS keys]`
  * the key must be in the BACKENDS
  * http request with the measurement matching the above principles will be forwarded to the backends

#### NODE

* `listenaddr`: proxy listen addr
* `db`: proxy db, client's db must be same with it
* `username`: proxy username
* `password`: proxy password
* `interval`: collect Statistics
* `idletimeout`: keep-alives wait time
* `writetracing`: enable logging for the write, default is 0
* `querytracing`: enable logging for the query, default is 0
* `datadir`: data dir to save .dat .rec, default is data
* `logpath`: log file path, default "" for stdout
* `httpsenabled`: enable https, default is false
* `httpscert`: the ssl certificate to use when https is enabled
* `httpskey`: use a separate private key location

Query Commands
--------

#### Unsupported commands

The following commands are forbid.

* `GRANT`
* `REVOKE`

#### Supported commands

Only support match the following commands.

* `.* from .*`
* `drop measurement`
* `show measurements`
* `show series`
* `show measurements`
* `show tag keys`
* `show tag values`
* `show field keys`
* `show retention policies`
* `create database`

License
-------

MIT.
