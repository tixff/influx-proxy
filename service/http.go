// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package service

import (
    "compress/gzip"
    "io/ioutil"
    "log"
    "net/http"
    "net/http/pprof"
    "strings"

    "github.com/chengshiwen/influx-proxy/backend"
)

type HttpService struct {
    db string
    username string
    password string
    ic *backend.InfluxCluster
}

func NewHttpService(ic *backend.InfluxCluster, nodecfg *backend.NodeConfig) (hs *HttpService) {
    hs = &HttpService{
        db: nodecfg.DB,
        username: nodecfg.Username,
        password: nodecfg.Password,
        ic: ic,
    }
    if hs.db != "" {
        log.Print("http database: ", hs.db)
    }
    return
}

func (hs *HttpService) checkAuth(req *http.Request) bool {
    if hs.username == "" && hs.password == "" {
        return true
    }
    q := req.URL.Query()
    if u, p := q.Get("u"), q.Get("p"); u == hs.username && p == hs.password {
        return true
    }
    if u, p, ok := req.BasicAuth(); ok && u == hs.username && p == hs.password {
        return true
    }
    return false
}

func (hs *HttpService) checkDatabase(q string) bool {
    q = strings.ToLower(strings.TrimSpace(q))
    return (strings.HasPrefix(q, "show") && strings.Contains(q, "databases")) || (strings.HasPrefix(q, "create") && strings.Contains(q, "database"))
}

func (hs *HttpService) Register(mux *http.ServeMux) {
    mux.HandleFunc("/ping", hs.HandlerPing)
    mux.HandleFunc("/query", hs.HandlerQuery)
    mux.HandleFunc("/write", hs.HandlerWrite)
    mux.HandleFunc("/debug/pprof/", pprof.Index)
    mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    version, err := hs.ic.Ping()
    if err != nil {
        panic("WTF")
        return
    }
    w.Header().Add("X-Influxdb-Version", version)
    w.WriteHeader(204)
    return
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    w.Header().Add("X-Influxdb-Version", backend.VERSION)

    if !hs.checkAuth(req) {
        w.WriteHeader(401)
        w.Write([]byte("authentication failed\n"))
        return
    }

    q := req.FormValue("q")
    db := req.FormValue("db")
    if hs.db != "" && !hs.checkDatabase(q) && db != hs.db {
        w.WriteHeader(404)
        w.Write([]byte("database forbidden\n"))
        return
    }

    err := hs.ic.Query(w, req)
    if err != nil {
        log.Printf("query error: %s, the query is %s, the client is %s\n", err, q, req.RemoteAddr)
        return
    }
    if hs.ic.QueryTracing {
        log.Printf("the query is %s, the client is %s\n", q, req.RemoteAddr)
    }

    return
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    w.Header().Add("X-Influxdb-Version", backend.VERSION)

    if !hs.checkAuth(req) {
        w.WriteHeader(401)
        w.Write([]byte("authentication failed\n"))
        return
    }

    if req.Method != "POST" {
        w.WriteHeader(405)
        w.Write([]byte("method not allow\n"))
        return
    }

    db := req.URL.Query().Get("db")
    if hs.db != "" && db != hs.db {
        w.WriteHeader(404)
        w.Write([]byte("database forbidden\n"))
        return
    }

    body := req.Body
    if req.Header.Get("Content-Encoding") == "gzip" {
        b, err := gzip.NewReader(req.Body)
        if err != nil {
            w.WriteHeader(400)
            w.Write([]byte("unable to decode gzip body\n"))
            return
        }
        defer b.Close()
        body = b
    }

    p, err := ioutil.ReadAll(body)
    if err != nil {
        w.WriteHeader(400)
        w.Write([]byte(err.Error()))
        return
    }

    precision := req.URL.Query().Get("precision")
    if precision == "" {
        precision = "ns"
    }
    err = hs.ic.Write(p, precision)
    if err == nil {
        w.WriteHeader(204)
    }
    if hs.ic.WriteTracing {
        log.Printf("write body received by handler: %s, the client is %s\n", p, req.RemoteAddr)
    }
    return
}
