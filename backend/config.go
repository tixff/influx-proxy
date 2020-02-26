// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
    "encoding/json"
    "errors"
    "log"
    "os"
)

const (
    VERSION = "1.2.3"
)

var (
    ErrIllegalConfig = errors.New("illegal config")
)

// listenaddr: proxy listen addr
// db: proxy db, client's db must be same with it
// username: proxy username
// password: proxy password
// interval: collect Statistics
// idletimeout: keep-alives wait time
// writetracing: enable logging for the write, default is 0
// querytracing: enable logging for the query, default is 0
// datadir: data dir to save .dat .rec, default is data
// logpath: log file path, default "" for stdout
// httpsenabled: enable https, default is false
// httpscert: the ssl certificate to use when https is enabled
// httpskey: use a separate private key location
type NodeConfig struct {
    ListenAddr   string
    DB           string
    Username     string
    Password     string
    Interval     int
    IdleTimeout  int
    WriteTracing int
    QueryTracing int
    DataDir      string
    LogPath      string
    HTTPSEnabled bool
    HTTPSCert    string
    HTTPSKey     string
}

// url: influxdb addr or other http backend which supports influxdb line protocol
// db: influxdb db
// username: influxdb username
// password: influxdb password
// interval: default config is 1000ms, wait 1 second write whether point count has bigger than maxrowlimit config
// timeout: default config is 10000ms, write timeout until 10 seconds
// timeoutquery: default config is 600000ms, query timeout until 600 seconds
// maxrowlimit: default config is 10000, wait 10000 points write
// checkinterval: default config is 1000ms, check backend active every 1 second
// rewriteinterval: default config is 10000ms, rewrite every 10 seconds
// writeonly: default 0
type BackendConfig struct {
    URL             string
    DB              string
    Username        string
    Password        string
    Interval        int
    Timeout         int
    TimeoutQuery    int
    MaxRowLimit     int
    CheckInterval   int
    RewriteInterval int
    WriteOnly       int
}

// KEYMAPS
// measurement: [BACKENDS keys], the key must be in the BACKENDS
// http request with the measurement matching the following principles will be forwarded to the backends
// exact match first:
//      for instance, we use cpu.load for measurement's name
//      The KEYMAPS has cpu and cpu.load keys, it will use the cpu.load corresponding backends
// prefix match then:
//      for instance, we use cpu.load for measurement's name
//      the KEYMAPS only has cpu key, it will use the cpu corresponding backends
// _default_ match finally:
//      if the measurement don't match the above principles, it will use the _default_ corresponding backends

// BACKENDS, KEYMAPS, NODE
type FileConfigSource struct {
    BACKENDS     map[string]BackendConfig
    KEYMAPS      map[string][]string
    NODE         NodeConfig
}

func NewFileConfigSource(cfgfile string) (fcs *FileConfigSource, err error) {
    fcs = &FileConfigSource{}
    file, err := os.Open(cfgfile)
    if err != nil {
        return
    }
    defer file.Close()
    dec := json.NewDecoder(file)
    err = dec.Decode(fcs)
    return
}

func (fcs *FileConfigSource) LoadNode() (nodecfg NodeConfig) {
    nodecfg = fcs.NODE
    if nodecfg.ListenAddr == "" {
        nodecfg.ListenAddr = ":7076"
    }
    if nodecfg.DataDir == "" {
        nodecfg.DataDir = "data"
    }
    return
}

func (fcs *FileConfigSource) LoadBackends() (backends map[string]*BackendConfig, err error) {
    backends = make(map[string]*BackendConfig)
    for name, val := range fcs.BACKENDS {
        cfg := &BackendConfig{
            URL: val.URL,
            DB: val.DB,
            Username: val.Username,
            Password: val.Password,
            Interval: val.Interval,
            Timeout: val.Timeout,
            TimeoutQuery: val.TimeoutQuery,
            MaxRowLimit: val.MaxRowLimit,
            CheckInterval: val.CheckInterval,
            RewriteInterval: val.RewriteInterval,
            WriteOnly: val.WriteOnly,
        }
        if cfg.Interval == 0 {
            cfg.Interval = 1000
        }
        if cfg.Timeout == 0 {
            cfg.Timeout = 10000
        }
        if cfg.TimeoutQuery == 0 {
            cfg.TimeoutQuery = 600000
        }
        if cfg.MaxRowLimit == 0 {
            cfg.MaxRowLimit = 10000
        }
        if cfg.CheckInterval == 0 {
            cfg.CheckInterval = 1000
        }
        if cfg.RewriteInterval == 0 {
            cfg.RewriteInterval = 10000
        }
        backends[name] = cfg
    }
    log.Printf("%d backends loaded from file", len(backends))
    return
}

func (fcs *FileConfigSource) LoadMeasurements() (m_map map[string][]string, err error) {
    m_map = fcs.KEYMAPS
    log.Printf("%d measurements loaded from file", len(m_map))
    return
}
