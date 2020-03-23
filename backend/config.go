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
    VERSION = "1.3.4"
)

var (
    ErrIllegalConfig = errors.New("illegal config")
)

// listenaddr: proxy listen addr
// db: proxy db, client's db must be same with it
// username: proxy username
// password: proxy password
// datadir: data dir to save .dat .rec, default is data
// logpath: log file path, default "" for stdout
// idletimeout: keep-alives wait time, default is 10000ms
// statinterval: interval to collect statistics, default is 10000ms
// writetracing: enable logging for the write, default is false
// querytracing: enable logging for the query, default is false
// httpsenabled: enable https, default is false
// httpscert: the ssl certificate to use when https is enabled
// httpskey: use a separate private key location
type NodeConfig struct {
    ListenAddr   string
    DB           string
    Username     string
    Password     string
    DataDir      string
    LogPath      string
    IdleTimeout  int
    StatInterval int
    WriteTracing bool
    QueryTracing bool
    HTTPSEnabled bool
    HTTPSCert    string
    HTTPSKey     string
}

// url: influxdb addr or other http backend which supports influxdb line protocol
// db: influxdb db
// username: influxdb username
// password: influxdb password
// flushsize: default config is 10000, wait 10000 points write
// flushtime: default config is 1000ms, wait 1 second write whether point count has bigger than flushsize config
// timeout: default config is 10000ms, write timeout until 10 seconds
// checkinterval: default config is 1000ms, check backend active every 1 second
// rewriteinterval: default config is 10000ms, rewrite every 10 seconds
// writeonly: default is false
type BackendConfig struct {
    URL             string
    DB              string
    Username        string
    Password        string
    FlushSize       int
    FlushTime       int
    Timeout         int
    CheckInterval   int
    RewriteInterval int
    WriteOnly       bool
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
    if nodecfg.IdleTimeout == 0 {
        nodecfg.IdleTimeout = 10000
    }
    if nodecfg.StatInterval == 0 {
        nodecfg.StatInterval = 10000
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
            FlushSize: val.FlushSize,
            FlushTime: val.FlushTime,
            Timeout: val.Timeout,
            CheckInterval: val.CheckInterval,
            RewriteInterval: val.RewriteInterval,
            WriteOnly: val.WriteOnly,
        }
        if cfg.FlushSize == 0 {
            cfg.FlushSize = 10000
        }
        if cfg.FlushTime == 0 {
            cfg.FlushTime = 1000
        }
        if cfg.Timeout == 0 {
            cfg.Timeout = 10000
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
