// Copyright 2018 BizSeer. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: corylanou, chengshiwen

package backend

import (
    "bytes"
    "compress/gzip"
    "errors"
    "io"
    "net/http"
    "regexp"
    "strings"

    "github.com/influxdata/influxdb1-client/models"
)

var (
    ErrNotOneSeries = errors.New("not one series error")
)

type InfluxQLExecutor struct {
}

func GzipCompress(b []byte) (cb []byte, err error) {
    var buf bytes.Buffer
    zip := gzip.NewWriter(&buf)
    n, err := zip.Write(b)
    if err != nil {
        return
    }
    if n != len(b) {
        err = io.ErrShortWrite
        return
    }
    err = zip.Close()
    cb = buf.Bytes()
    return
}

func (iqe *InfluxQLExecutor) Query(w http.ResponseWriter, req *http.Request, backends map[string]BackendAPI, zone string) (err error) {
    // remove support of query parameter `chunked`
    req.Form.Del("chunked")
    var header http.Header
    var status int
    var bodies [][]byte
    for _, api := range backends {
        if api.GetZone() != zone {
            continue
        }
        if !api.IsActive() || api.IsWriteOnly() {
            continue
        }
        _header, _status, _body, _err := api.QueryResp(req)
        if _status >= http.StatusBadRequest {
            copyHeader(w.Header(), _header)
            w.WriteHeader(_status)
            w.Write(_body)
            return
        }
        if _err != nil {
            err = _err
            return
        }
        header = _header
        status = _status
        bodies = append(bodies, _body)
    }

    var rbody []byte
    var _err error
    q := strings.ToLower(strings.TrimSpace(req.FormValue("q")))
    if matched, _ := regexp.MatchString("^show\\s+retention\\s+policies", q); matched {
        rbody, _err = iqe.concatByValues(bodies)
    } else if matched, _ := regexp.MatchString("^show\\s+series|^show\\s+measurements", q); matched {
        rbody, _err = iqe.reduceByValues(bodies)
    } else if matched, _ := regexp.MatchString("^show\\s+tag\\s+keys|^show\\s+tag\\s+values|^show\\s+field\\s+keys", q); matched {
        rbody, _err = iqe.reduceBySeries(bodies)
    }
    if _err != nil {
        err = _err
        return
    }

    copyHeader(w.Header(), header)
    w.WriteHeader(status)
    if header.Get("Content-Encoding") == "gzip" {
        gzipBody, _err := GzipCompress(rbody)
        if _err != nil {
            err = _err
            return
        } else {
            w.Write(gzipBody)
        }
    } else {
        w.Write(rbody)
    }
    return
}

func (iqe *InfluxQLExecutor) concatByValues(bodies [][]byte) (rbody []byte, err error) {
    var series []*models.Row
    var values [][]interface{}
    for _, body := range bodies {
        series, err = SeriesFromResultSetBytes(body)
        if err != nil {
            return
        }
        if len(series) != 1 {
            err = ErrNotOneSeries
            return
        } else {
            for _, value := range series[0].Values {
                values = append(values, value)
            }
        }
    }
    series[0].Values = values
    rbody, err = ResultSetBytesFromSeries(series)
    return
}

func (iqe *InfluxQLExecutor) reduceByValues(bodies [][]byte) (rbody []byte, err error) {
    var series []*models.Row
    var values [][]interface{}
    valuesMap := make(map[string][]interface{})
    for _, body := range bodies {
        series, err = SeriesFromResultSetBytes(body)
        if err != nil {
            return
        }
        if len(series) != 1 {
            err = ErrNotOneSeries
            return
        } else {
            for _, value := range series[0].Values {
                key := value[0].(string)
                if !strings.HasPrefix(key, "influxdb.cluster.statistics") {
                    valuesMap[key] = value
                }
            }
        }
    }
    for _, value := range valuesMap {
        values = append(values, value)
    }
    series[0].Values = values
    rbody, err = ResultSetBytesFromSeries(series)
    return
}

func (iqe *InfluxQLExecutor) reduceBySeries(bodies [][]byte) (rbody []byte, err error) {
    var series []*models.Row
    seriesMap := make(map[string]*models.Row)
    for _, body := range bodies {
        _series, _err := SeriesFromResultSetBytes(body)
        if _err != nil {
            err = _err
            return
        }
        for _, serie := range _series {
            if serie.Name != "influxdb.cluster.statistics" {
                seriesMap[serie.Name] = serie
            }
        }
    }
    for _, serie := range seriesMap {
        series = append(series, serie)
    }
    rbody, err = ResultSetBytesFromSeries(series)
    return
}
