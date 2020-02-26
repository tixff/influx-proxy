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
    Version     bool
    GitCommit   string
    BuildTime   string
)

func init() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
    flag.StringVar(&ConfigFile, "config", "proxy.json", "proxy config file")
    flag.BoolVar(&Version, "version", false, "proxy version")
    flag.Parse()
}

func initLog(logPath string) {
    if logPath == "" {
        log.SetOutput(os.Stdout)
    } else {
        log.SetOutput(&lumberjack.Logger{
            Filename:   logPath,
            MaxSize:    100,
            MaxBackups: 5,
            MaxAge:     7,
        })
    }
}

func createDataDir(dataDir string)  {
    exist, err := pathExists(dataDir)
    if err != nil {
        log.Fatalln("check data dir error")
    }
    if !exist {
        err = os.MkdirAll(dataDir, os.ModePerm)
        if err != nil {
            log.Fatalln("create data dir error")
        }
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

    fcs, err := backend.NewFileConfigSource(ConfigFile)
    if err != nil {
        fmt.Println("config source load failed")
        return
    }
    nodecfg := fcs.LoadNode()

    initLog(nodecfg.LogPath)
    createDataDir(nodecfg.DataDir)

    ic := backend.NewInfluxCluster(fcs, &nodecfg)
    ic.LoadConfig()

    mux := http.NewServeMux()
    NewHttpService(ic, &nodecfg).Register(mux)

    log.Printf("http service start")
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
