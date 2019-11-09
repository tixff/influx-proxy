// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
    "errors"
    "flag"
    "log"
    "net/http"
    "os"
    "time"

    "github.com/chengshiwen/influx-proxy/backend"
    "gopkg.in/natefinch/lumberjack.v2"
)

var (
    ErrConfig   = errors.New("config parse error")
    ConfigFile  string
    NodeName    string
    LogPath     string
    DataDir     string
)

func init() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

    flag.StringVar(&ConfigFile, "config", "proxy.json", "proxy config file")
    flag.StringVar(&NodeName, "node", "l1", "node name")
    flag.StringVar(&LogPath, "log-path", "proxy.log", "log file path")
    flag.StringVar(&DataDir, "data-dir", "data", "data dir to save .dat .rec")
    flag.Parse()
}

func initLog() {
    if LogPath == "" {
        log.SetOutput(os.Stdout)
    } else {
        log.SetOutput(&lumberjack.Logger{
            Filename:   LogPath,
            MaxSize:    100,
            MaxBackups: 5,
            MaxAge:     7,
        })
    }
}

func pathExists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil {
        return true, nil
    }
    if os.IsNotExist(err) {
        return false, nil
    }
    return false, err
}

func main() {
    initLog()

    exist, err := pathExists(DataDir)
    if err != nil {
        log.Println("check data dir error")
        return
    }
    if !exist {
        err = os.MkdirAll(DataDir, os.ModePerm)
        if err != nil {
            log.Println("create data dir error")
            return
        }
    }

    fcs := backend.NewFileConfigSource(ConfigFile, NodeName)

    nodecfg, err := fcs.LoadNode()
    if err != nil {
        log.Printf("config source load failed.")
        return
    }

    ic := backend.NewInfluxCluster(fcs, &nodecfg, DataDir)
    ic.LoadConfig()

    mux := http.NewServeMux()
    NewHttpService(ic, nodecfg.DB).Register(mux)

    log.Printf("http service start.")
    server := &http.Server{
        Addr:        nodecfg.ListenAddr,
        Handler:     mux,
        IdleTimeout: time.Duration(nodecfg.IdleTimeout) * time.Second,
    }
    if nodecfg.IdleTimeout <= 0 {
        server.IdleTimeout = 10 * time.Second
    }
    err = server.ListenAndServe()
    if err != nil {
        log.Print(err)
        return
    }
}
