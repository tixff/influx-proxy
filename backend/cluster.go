// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
    "bytes"
    "errors"
    "io"
    "log"
    "net/http"
    "os"
    "regexp"
    "strings"
    "sync"
    "sync/atomic"
    "time"
    "unsafe"

    "github.com/chengshiwen/influx-proxy/monitor"
)

var (
    ErrClosed          = errors.New("write in a closed file")
    ErrBackendNotExist = errors.New("use a backend not exists")
    ErrQueryForbidden  = errors.New("query forbidden")
    ErrNotClusterQuery = errors.New("not a cluster query")
)

var StatisticsMetricName = "influxdb.cluster.statistics"

type InfluxCluster struct {
    lock           sync.RWMutex
    query_executor *InfluxQLExecutor
    ForbiddenQuery []*regexp.Regexp
    ObligatedQuery []*regexp.Regexp
    ExecutedQuery  []*regexp.Regexp
    cfgsrc         *FileConfigSource
    backends       map[string]BackendAPI
    m2bs           map[string][]BackendAPI // measurements to backends
    stats          *Statistics
    counter        *Statistics
    ticker         *time.Ticker
    defaultTags    map[string]string
    datadir        string
    WriteTracing   bool
    QueryTracing   bool
}

type Statistics struct {
    QueryRequests        int64
    QueryRequestsFail    int64
    WriteRequests        int64
    WriteRequestsFail    int64
    PingRequests         int64
    PingRequestsFail     int64
    PointsWritten        int64
    PointsWrittenFail    int64
    WriteRequestDuration int64
    QueryRequestDuration int64
}

func NewInfluxCluster(cfgsrc *FileConfigSource, nodecfg *NodeConfig) (ic *InfluxCluster) {
    ic = &InfluxCluster{
        query_executor: &InfluxQLExecutor{},
        cfgsrc:         cfgsrc,
        stats:          &Statistics{},
        counter:        &Statistics{},
        ticker:         time.NewTicker(time.Millisecond * time.Duration(nodecfg.StatInterval)),
        defaultTags:    map[string]string{"addr": nodecfg.ListenAddr},
        datadir:        nodecfg.DataDir,
        WriteTracing:   nodecfg.WriteTracing,
        QueryTracing:   nodecfg.QueryTracing,
    }
    ic.query_executor.ic = ic
    host, err := os.Hostname()
    if err != nil {
        log.Println(err)
    }
    ic.defaultTags["host"] = host

    ic.ForbidQuery(ForbidCmds)
    ic.EnsureQuery(SupportCmds)
    ic.ExecuteQuery(ExecutorCmds)

    // feature
    go ic.statistics()
    return
}

func (ic *InfluxCluster) statistics() {
    // how to quit
    for {
        <-ic.ticker.C
        ic.Flush()
        ic.counter = (*Statistics)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&ic.stats)),
            unsafe.Pointer(ic.counter)))
        err := ic.WriteStatistics()
        if err != nil {
            log.Println(err)
        }
    }
}

func (ic *InfluxCluster) Flush() {
    ic.counter.QueryRequests = 0
    ic.counter.QueryRequestsFail = 0
    ic.counter.WriteRequests = 0
    ic.counter.WriteRequestsFail = 0
    ic.counter.PingRequests = 0
    ic.counter.PingRequestsFail = 0
    ic.counter.PointsWritten = 0
    ic.counter.PointsWrittenFail = 0
    ic.counter.WriteRequestDuration = 0
    ic.counter.QueryRequestDuration = 0
}

func (ic *InfluxCluster) WriteStatistics() (err error) {
    metric := &monitor.Metric{
        Name: StatisticsMetricName,
        Tags: ic.defaultTags,
        Fields: map[string]interface{}{
            "statQueryRequest":         ic.counter.QueryRequests,
            "statQueryRequestFail":     ic.counter.QueryRequestsFail,
            "statWriteRequest":         ic.counter.WriteRequests,
            "statWriteRequestFail":     ic.counter.WriteRequestsFail,
            "statPingRequest":          ic.counter.PingRequests,
            "statPingRequestFail":      ic.counter.PingRequestsFail,
            "statPointsWritten":        ic.counter.PointsWritten,
            "statPointsWrittenFail":    ic.counter.PointsWrittenFail,
            "statQueryRequestDuration": ic.counter.QueryRequestDuration,
            "statWriteRequestDuration": ic.counter.WriteRequestDuration,
        },
        Time: time.Now(),
    }
    line, err := metric.ParseToLine()
    if err != nil {
        return
    }
    return ic.Write([]byte(line + "\n"), "ns")
}

func (ic *InfluxCluster) ForbidQuery(cmds []string) {
    ic.lock.Lock()
    defer ic.lock.Unlock()
    for _, cmd := range cmds {
        r, err := regexp.Compile(cmd)
        if err != nil {
            panic(err)
            return
        }
        ic.ForbiddenQuery = append(ic.ForbiddenQuery, r)
    }
}

func (ic *InfluxCluster) EnsureQuery(cmds []string) {
    ic.lock.Lock()
    defer ic.lock.Unlock()
    for _, cmd := range cmds {
        r, err := regexp.Compile(cmd)
        if err != nil {
            panic(err)
            return
        }
        ic.ObligatedQuery = append(ic.ObligatedQuery, r)
    }
}

func (ic *InfluxCluster) ExecuteQuery(cmds []string) {
    ic.lock.Lock()
    defer ic.lock.Unlock()
    for _, cmd := range cmds {
        r, err := regexp.Compile(cmd)
        if err != nil {
            panic(err)
            return
        }
        ic.ExecutedQuery = append(ic.ExecutedQuery, r)
    }
}

func (ic *InfluxCluster) loadBackends() (backends map[string]BackendAPI, err error) {
    backends = make(map[string]BackendAPI)

    bkcfgs, err := ic.cfgsrc.LoadBackends()
    if err != nil {
        return
    }

    for name, cfg := range bkcfgs {
        backends[name], err = NewBackends(cfg, name, ic.datadir)
        if err != nil {
            log.Printf("create backend error: %s", err)
            return
        }
    }

    return
}

func (ic *InfluxCluster) loadMeasurements(backends map[string]BackendAPI) (m2bs map[string][]BackendAPI, err error) {
    m2bs = make(map[string][]BackendAPI)

    m_map, err := ic.cfgsrc.LoadMeasurements()
    if err != nil {
        return
    }

    for name, bs_names := range m_map {
        var bss []BackendAPI
        for _, bs_name := range bs_names {
            bs, ok := backends[bs_name]
            if !ok {
                err = ErrBackendNotExist
                log.Println(bs_name, err)
                continue
            }
            bss = append(bss, bs)
        }
        m2bs[name] = bss
    }
    return
}

func (ic *InfluxCluster) LoadConfig() (err error) {
    backends, err := ic.loadBackends()
    if err != nil {
        return
    }

    m2bs, err := ic.loadMeasurements(backends)
    if err != nil {
        return
    }

    ic.lock.Lock()
    orig_backends := ic.backends
    ic.backends = backends
    ic.m2bs = m2bs
    ic.lock.Unlock()

    for name, bs := range orig_backends {
        err = bs.Close()
        if err != nil {
            log.Printf("fail in close backend %s", name)
        }
    }
    return
}

func (ic *InfluxCluster) Ping() (version string, err error) {
    atomic.AddInt64(&ic.stats.PingRequests, 1)
    version = VERSION
    return
}

func (ic *InfluxCluster) CheckMeasurementQuery(q string) (err error) {
    ic.lock.RLock()
    defer ic.lock.RUnlock()

    if len(ic.ForbiddenQuery) != 0 {
        for _, fq := range ic.ForbiddenQuery {
            if fq.MatchString(q) {
                return ErrQueryForbidden
            }
        }
    }

    if len(ic.ObligatedQuery) != 0 {
        for _, pq := range ic.ObligatedQuery {
            if pq.MatchString(q) {
                return
            }
        }
        return ErrQueryForbidden
    }

    return
}

func (ic *InfluxCluster) CheckClusterQuery(q string) (err error) {
    ic.lock.RLock()
    defer ic.lock.RUnlock()

    if len(ic.ExecutedQuery) != 0 {
        for _, pq := range ic.ExecutedQuery {
            if pq.MatchString(q) {
                return
            }
        }
        return ErrNotClusterQuery
    }

    return
}

func (ic *InfluxCluster) GetBackends(key string) (backends []BackendAPI, ok bool) {
    ic.lock.RLock()
    defer ic.lock.RUnlock()

    backends, ok = ic.m2bs[key]
    // match use prefix
    if !ok {
        for k, v := range ic.m2bs {
            if strings.HasPrefix(key, k) {
                backends = v
                ok = true
                break
            }
        }
    }

    if !ok {
        backends, ok = ic.m2bs["_default_"]
    }

    return
}

func (ic *InfluxCluster) Query(w http.ResponseWriter, req *http.Request) (err error) {
    atomic.AddInt64(&ic.stats.QueryRequests, 1)
    defer func(start time.Time) {
        atomic.AddInt64(&ic.stats.QueryRequestDuration, time.Since(start).Nanoseconds())
    }(time.Now())

    switch req.Method {
    case "GET", "POST":
    default:
        w.WriteHeader(400)
        w.Write([]byte("illegal method\n"))
        atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
        return
    }

    // TODO: multi queries in q?
    q := strings.TrimSpace(req.FormValue("q"))
    if q == "" {
        w.WriteHeader(400)
        w.Write([]byte("empty query\n"))
        atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
        return
    }

    err = ic.CheckMeasurementQuery(q)
    if err != nil {
        err = ic.CheckClusterQuery(q)
        if err == nil {
            err = ic.query_executor.Query(w, req)
            if err != nil {
                log.Print("query executor error: ", err)
                w.WriteHeader(400)
                w.Write([]byte("query executor error\n"))
                atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
            }
            return
        }
        w.WriteHeader(400)
        w.Write([]byte("query forbidden\n"))
        atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
        return
    }

    key, err := GetMeasurementFromInfluxQL(q)
    if err != nil {
        log.Printf("can't get measurement: %s\n", q)
        w.WriteHeader(400)
        w.Write([]byte("can't get measurement\n"))
        atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
        return
    }

    apis, ok := ic.GetBackends(key)
    if !ok {
        log.Printf("unknown measurement: %s, the query is %s\n", key, q)
        w.WriteHeader(400)
        w.Write([]byte("unknown measurement\n"))
        atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
        return
    }

    // pass non-active and write-only.
    for _, api := range apis {
        if !api.IsActive() || api.IsWriteOnly() {
            continue
        }
        err = api.Query(w, req)
        if err == nil {
            return
        }
    }

    w.WriteHeader(400)
    if err == nil {
        w.Write([]byte("backends not active\n"))
    } else {
        log.Print("query error: ", err)
        w.Write([]byte("query error\n"))
    }
    atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
    return
}

// Wrong in one row will not stop others.
// So don't try to return error, just print it.
func (ic *InfluxCluster) WriteRow(line []byte) {
    atomic.AddInt64(&ic.stats.PointsWritten, 1)

    // empty line, ignore it.
    if len(line) == 0 {
        return
    }

    key, err := ScanKey(line)
    if err != nil {
        log.Printf("scan key error: %s\n", err)
        atomic.AddInt64(&ic.stats.PointsWrittenFail, 1)
        return
    }

    bs, ok := ic.GetBackends(key)
    if !ok {
        log.Printf("new measurement: %s\n", key)
        atomic.AddInt64(&ic.stats.PointsWrittenFail, 1)
        return
    }

    // don't block here for a long time, we just have one worker.
    for _, b := range bs {
        err = b.Write(line)
        if err != nil {
            log.Printf("cluster write fail: %s\n", key)
            atomic.AddInt64(&ic.stats.PointsWrittenFail, 1)
            return
        }
    }
    return
}

func (ic *InfluxCluster) Write(p []byte, precision string) (err error) {
    atomic.AddInt64(&ic.stats.WriteRequests, 1)
    defer func(start time.Time) {
        atomic.AddInt64(&ic.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
    }(time.Now())

    buf := bytes.NewBuffer(p)

    var line []byte
    for {
        line, err = buf.ReadBytes('\n')
        switch err {
        default:
            log.Printf("error: %s\n", err)
            atomic.AddInt64(&ic.stats.WriteRequestsFail, 1)
            return
        case io.EOF, nil:
            err = nil
        }

        if len(line) == 0 {
            break
        }

        line = LineToNano(line, precision)
        ic.WriteRow(line)
    }

    return
}

func (ic *InfluxCluster) Close() (err error) {
    ic.lock.RLock()
    defer ic.lock.RUnlock()
    for name, bs := range ic.backends {
        err = bs.Close()
        if err != nil {
            log.Printf("fail in close backend %s", name)
        }
    }
    return
}
