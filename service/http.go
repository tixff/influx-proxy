// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package service

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"

	"github.com/chengshiwen/influx-proxy/backend"
	gzip "github.com/klauspost/pgzip"
)

type HttpService struct { // nolint:golint
	ic       *backend.InfluxCluster
	username string
	password string
}

func NewHttpService(ic *backend.InfluxCluster, nodecfg *backend.NodeConfig) (hs *HttpService) { // nolint:golint
	hs = &HttpService{
		ic:       ic,
		username: nodecfg.Username,
		password: nodecfg.Password,
	}
	if hs.ic.DB != "" {
		log.Print("http database: ", hs.ic.DB)
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

func (hs *HttpService) Register(mux *http.ServeMux) {
	mux.HandleFunc("/ping", hs.HandlerPing)
	mux.HandleFunc("/query", hs.HandlerQuery)
	mux.HandleFunc("/write", hs.HandlerWrite)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	version, _ := hs.ic.Ping()
	w.Header().Add("X-Influxdb-Version", version)
	w.WriteHeader(204)
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
	err := hs.ic.Query(w, req)
	if err != nil {
		log.Printf("query error: %s, the query is %s, the client is %s\n", err, q, req.RemoteAddr)
		return
	}
	if hs.ic.QueryTracing {
		log.Printf("the query is %s, the client is %s\n", q, req.RemoteAddr)
	}
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

	precision := req.URL.Query().Get("precision")
	if precision == "" {
		precision = "ns"
	}
	db := req.URL.Query().Get("db")
	if db == "" {
		w.WriteHeader(400)
		w.Write([]byte("database not found\n"))
		return
	}
	if hs.ic.DB != "" && db != hs.ic.DB {
		w.WriteHeader(400)
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

	err = hs.ic.Write(p, precision)
	if err == nil {
		w.WriteHeader(204)
	}
	if hs.ic.WriteTracing {
		log.Printf("write body received by handler: %s, the client is %s\n", p, req.RemoteAddr)
	}
}
