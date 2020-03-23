// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
    "bytes"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "testing"
    "time"
)

func TestScanKey(t *testing.T) {
    var points []string = []string{
        "cpu,host=server01,region=uswest value=1 1434055562000000000",
        "cpu value=3,value2=4 1434055562000010000",
        "temper\\ ature,machine=unit42,type=assembly internal=32,external=100 1434055562000000035",
        "temper\\,ature,machine=unit143,type=assembly internal=22,external=130 1434055562005000035",
    }
    var keys []string = []string{
        "cpu",
        "cpu",
        "temper ature",
        "temper,ature",
    }

    var key string
    var err error
    for i, s := range points {
        key, err = ScanKey([]byte(s))
        if err != nil {
            t.Errorf("error: %s", err)
            return
        }
        if key != keys[i] {
            t.Errorf("quota test failed: %s, %s", key, keys[i])
            return
        }
    }

    return
}

func BenchmarkScanKey(b *testing.B) {
    buf := &bytes.Buffer{}
    for i := 0; i < b.N; i++ {
        fmt.Fprintf(buf, "%s%d,a=%d,b=2 c=3 10000\n", "name", i, i)
    }
    b.ResetTimer()

    var err error
    var line []byte
    for {
        line, err = buf.ReadBytes('\n')
        switch err {
        default:
            b.Error(err)
            return
        case io.EOF, nil:
        }

        if len(line) == 0 {
            break
        }

        line = bytes.TrimRight(line, " \t\r\n")
        _, err = ScanKey(line)
        if err != nil {
            b.Error(err)
            return
        }
    }
}

func CreateTestInfluxCluster() (ic *InfluxCluster, err error) {
    fileConfig := &FileConfigSource{}
    nodeConfig := &NodeConfig{DataDir:"../data/test", Interval:10}
    ic = NewInfluxCluster(fileConfig, nodeConfig)
    backends := make(map[string]BackendAPI)
    bkcfgs := make(map[string]*BackendConfig)
    cfg, _ := CreateTestBackendConfig("test1")
    bkcfgs["test1"] = cfg
    cfg, _ = CreateTestBackendConfig("test2")
    bkcfgs["test2"] = cfg
    cfg, _ = CreateTestBackendConfig("write_only")
    cfg.WriteOnly = true
    bkcfgs["write_only"] = cfg
    for name, cfg := range bkcfgs {
        backends[name], err = NewBackends(cfg, name, ic.datadir)
        if err != nil {
            return
        }
    }
    ic.backends = backends
    m2bs := make(map[string][]BackendAPI)
    m2bs["cpu"] = append(m2bs["cpu"], backends["write_only"], backends["test1"])
    m2bs["write_only"] = append(m2bs["write_only"], backends["write_only"])
    ic.m2bs = m2bs

    return
}

func TestInfluxdbClusterWrite(t *testing.T) {
    ic, err := CreateTestInfluxCluster()
    if err != nil {
        t.Error(err)
        return
    }
    tests := []struct {
        name string
        args []byte
        unit string
        want error
    }{
        {
            name: "cpu",
            args: []byte("cpu value=1,value2=2 1434055562000010000"),
            want: nil,
        },
        {
            name: "cpu",
            args: []byte("cpu value=3,value2=4 1434055562000010000"),
            unit: "ns",
            want: nil,
        },
        {
            name: "cpu.load",
            args: []byte("cpu.load value=3,value2=4 1434055562000010"),
            unit: "u",
            want: nil,
        },
        {
            name: "load.cpu",
            args: []byte("load.cpu value=3,value2=4 1434055562000"),
            unit: "ms",
            want: nil,
        },
        {
            name: "test",
            args: []byte("test value=3,value2=4 1434055562"),
            unit: "s",
        },
    }
    for _, tt := range tests {
        err := ic.Write(tt.args, tt.unit)
        if err != nil {
            t.Error(tt.name, err)
            continue
        }
    }
    time.Sleep(time.Second)
}
func TestInfluxdbClusterPing(t *testing.T) {
    ic, err := CreateTestInfluxCluster()
    if err != nil {
        t.Error(err)
        return
    }
    version, err := ic.Ping()
    if err != nil {
        t.Error(err)
        return
    }
    if version == "" {
        t.Error("empty version")
        return
    }
    time.Sleep(time.Second)
}

func TestInfluxdbClusterQuery(t *testing.T) {
    ic, err := CreateTestInfluxCluster()
    if err != nil {
        t.Error(err)
        return
    }
    w := NewDummyResponseWriter()
    w.Header().Add("X-Influxdb-Version", VERSION)
    q := url.Values{}
    q.Set("db", "test")

    tests := []struct {
        name  string
        query string
        want  int
    }{
        {
            name:  "cpu",
            query: "SELECT * from cpu where time < now() - 1m",
            want:  200,
        },
        {
            name:  "test",
            query: "SELECT cpu_load from test",
            want:  400,
        },
        {
            name:  "cpu_load",
            query: " select cpu_load from cpu",
            want:  200,
        },
        {
            name:  "cpu.load",
            query: " select cpu_load from \"cpu.load\" WHERE time > now() - 1m",
            want:  200,
        },
        {
            name:  "load.cpu",
            query: " select cpu_load from \"load.cpu\" WHERE time > now() - 1m",
            want:  400,
        },
        {
            name:  "show_tag_keys",
            query: "SHOW tag keys from \"cpu\" ",
            want:  200,
        },
        {
            name:  "show_tag_values",
            query: "SHOW tag values WITH key = \"host\"",
            want:  200,
        },
        {
            name:  "show_field_keys",
            query: "SHOW field KEYS from \"cpu\" ",
            want:  200,
        },
        {
            name:  "delete_cpu",
            query: " DELETE FROM \"cpu\" WHERE time < '2000-01-01T00:00:00Z'",
            want:  400,
        },
        {
            name:  "show_series",
            query: "show series",
            want:  200,
        },
        {
            name:  "show_measurements",
            query: "SHOW measurements ",
            want:  200,
        },
        {
            name:  "show_retention_policies",
            query: " SHOW retention policies limit 10",
            want:  200,
        },
        {
            name:  "cpu.load_with_host1",
            query: " select cpu_load from \"cpu.load\" WHERE host =~ /^$/",
            want:  200,
        },
        {
            name:  "cpu.load_with_host2",
            query: " select cpu_load from \"cpu.load\" WHERE host =~ /^()$/",
            want:  200,
        },
        {
            name:  "cpu.load_into_from",
            query: "select * into \"cpu.load_new\" from \"cpu.load\"",
            want:  400,
        },
        {
            name:  "cpu.load_into_from_group_by",
            query: "select * into \"cpu.load_new\" from \"cpu.load\" GROUP BY *",
            want:  400,
        },
        {
            name:  "write.only",
            query: " select cpu_load from write_only",
            want:  400,
        },
        {
            name:  "drop_measurement",
            query: "DROP measurement \"cpu.load\"",
            want:  400,
        },
    }

    for _, tt := range tests {
        q.Set("q", tt.query)
        req, _ := http.NewRequest("GET", "http://localhost:7076/query?"+q.Encode(), nil)
        req.URL.Query()
        err = ic.Query(w, req)
        if w.status != tt.want {
            t.Error(tt.name, err, w.status)
        }
        w.buffer.Reset()
    }
}
