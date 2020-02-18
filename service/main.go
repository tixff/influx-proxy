// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
    "errors"
    "flag"
    "fmt"
    "log"
    "net/http"
    "os"
    "runtime"
    "time"

    "github.com/chengshiwen/influx-proxy/backend"
    "gopkg.in/natefinch/lumberjack.v2"
)

var (
    ErrConfig   = errors.New("config parse error")
    ConfigFile  string
    DataDir     string
    LogPath     string
    Version     bool

    GitCommit   string
    BuildTime   string
)

func init() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

    flag.StringVar(&ConfigFile, "config", "proxy.json", "proxy config file")
    flag.StringVar(&DataDir, "data-dir", "data", "data dir to save .dat .rec")
    flag.StringVar(&LogPath, "log-path", "", "log file path (default \"\")")
    flag.BoolVar(&Version, "version", false, "proxy version")
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
    if Version {
        fmt.Printf("Version:    %s\n", backend.VERSION)
        fmt.Printf("Git commit: %s\n", GitCommit)
        fmt.Printf("Go version: %s\n", runtime.Version())
        fmt.Printf("Build time: %s\n", BuildTime)
        fmt.Printf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
        return
    }

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

    fcs, err := backend.NewFileConfigSource(ConfigFile)
    if err != nil {
        log.Printf("config source load failed.")
        return
    }
    nodecfg := fcs.LoadNode()

    ic := backend.NewInfluxCluster(fcs, &nodecfg, DataDir)
    ic.LoadConfig()

    mux := http.NewServeMux()
    NewHttpService(ic, &nodecfg).Register(mux)

    log.Printf("http service start.")
    server := &http.Server{
        Addr:        nodecfg.ListenAddr,
        Handler:     mux,
        IdleTimeout: time.Duration(nodecfg.IdleTimeout) * time.Second,
    }
    if nodecfg.IdleTimeout <= 0 {
        server.IdleTimeout = 10 * time.Second
    }
    if nodecfg.HTTPSEnabled {
        err = server.ListenAndServeTLS(nodecfg.HTTPSCert, nodecfg.HTTPSKey)
    } else {
        err = server.ListenAndServe()
    }
    if err != nil {
        log.Print(err)
        return
    }
}
