// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"encoding/json"
	"log"
	"os"
)

const (
	Version = "1.4.6"
)

type NodeConfig struct {
	ListenAddr   string `json:"listen_addr"`
	DB           string `json:"db"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	DataDir      string `json:"data_dir"`
	LogPath      string `json:"log_path"`
	IdleTimeout  int    `json:"idle_timeout"`
	StatInterval int    `json:"stat_interval"`
	WriteTracing bool   `json:"write_tracing"`
	QueryTracing bool   `json:"query_tracing"`
	HTTPSEnabled bool   `json:"https_enabled"`
	HTTPSCert    string `json:"https_cert"`
	HTTPSKey     string `json:"https_key"`
}

type BackendConfig struct { // nolint:golint
	URL             string `json:"url"`
	DB              string `json:"db"`
	Username        string `json:"username"`
	Password        string `json:"password"`
	FlushSize       int    `json:"flush_size"`
	FlushTime       int    `json:"flush_time"`
	Timeout         int    `json:"timeout"`
	CheckInterval   int    `json:"check_interval"`
	RewriteInterval int    `json:"rewrite_interval"`
	ConnPoolSize    int    `json:"conn_pool_size"`
	WriteOnly       bool   `json:"write_only"`
}

// KEYMAPS
// measurement: [BACKENDS keys], the key must be in the BACKENDS

type FileConfigSource struct {
	BACKENDS map[string]BackendConfig
	KEYMAPS  map[string][]string
	NODE     NodeConfig
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
			URL:             val.URL,
			DB:              val.DB,
			Username:        val.Username,
			Password:        val.Password,
			FlushSize:       val.FlushSize,
			FlushTime:       val.FlushTime,
			Timeout:         val.Timeout,
			CheckInterval:   val.CheckInterval,
			RewriteInterval: val.RewriteInterval,
			ConnPoolSize:    val.ConnPoolSize,
			WriteOnly:       val.WriteOnly,
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
		if cfg.ConnPoolSize == 0 {
			cfg.ConnPoolSize = 20
		}
		backends[name] = cfg
	}
	log.Printf("%d backends loaded from file", len(backends))
	return
}

func (fcs *FileConfigSource) LoadMeasurements() (m_map map[string][]string, err error) { // nolint:golint
	m_map = fcs.KEYMAPS
	log.Printf("%d measurements loaded from file", len(m_map))
	return
}
