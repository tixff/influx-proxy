// Copyright 2018 BizSeer. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: corylanou, chengshiwen

package backend

import (
    "bytes"
    "compress/gzip"
    "errors"
    "fmt"
    "io"
    "net/http"
    "regexp"
    "strings"

    "github.com/influxdata/influxdb1-client/models"
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

func (iqe *InfluxQLExecutor) Query(w http.ResponseWriter, req *http.Request, backends map[string]BackendAPI) (err error) {
    // remove support of query parameter `chunked`
    req.Form.Del("chunked")
    var header http.Header
    var status int
    var bodies [][]byte
    var inactive int
    for _, api := range backends {
        if api.IsWriteOnly() {
            continue
        }
        if !api.IsActive() {
            inactive++
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
    var rerr error
    if inactive > 0 {
        rerr = errors.New(fmt.Sprintf("%d/%d backends not active", inactive, inactive + len(bodies)))
    }
    if len(bodies) == 0 {
        rbody, err = ResultSetBytesFromSeriesAndError(nil, rerr)
        status = http.StatusOK
    } else {
        q := strings.ToLower(strings.TrimSpace(req.FormValue("q")))
        if matched, _ := regexp.MatchString("^show\\s+retention\\s+policies", q); matched {
            rbody, err = iqe.concatByValues(bodies, rerr)
        } else if matched, _ := regexp.MatchString("^show\\s+series|^show\\s+measurements", q); matched {
            rbody, err = iqe.reduceByValues(bodies, rerr)
        } else if matched, _ := regexp.MatchString("^show\\s+tag\\s+keys|^show\\s+tag\\s+values|^show\\s+field\\s+keys", q); matched {
            rbody, err = iqe.reduceBySeries(bodies, rerr)
        }
    }
    if err != nil {
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

func (iqe *InfluxQLExecutor) concatByValues(bodies [][]byte, rerr error) (rbody []byte, err error) {
    var series []*models.Row
    var values [][]interface{}
    for _, body := range bodies {
        _series, _err := SeriesFromResultSetBytes(body)
        if _err != nil {
            err = _err
            return
        }
        if len(_series) == 1 {
            series = _series
            for _, value := range _series[0].Values {
                values = append(values, value)
            }
        }
    }
    if len(series) == 1 {
        series[0].Values = values
    }
    rbody, err = ResultSetBytesFromSeriesAndError(series, rerr)
    return
}

func (iqe *InfluxQLExecutor) reduceByValues(bodies [][]byte, rerr error) (rbody []byte, err error) {
    var series []*models.Row
    var values [][]interface{}
    valuesMap := make(map[string][]interface{})
    for _, body := range bodies {
        _series, _err := SeriesFromResultSetBytes(body)
        if _err != nil {
            err = _err
            return
        }
        if len(_series) == 1 {
            series = _series
            for _, value := range _series[0].Values {
                key := value[0].(string)
                if !strings.HasPrefix(key, StatisticsMetricName) {
                    valuesMap[key] = value
                }
            }
        }
    }
    if len(series) == 1 {
        for _, value := range valuesMap {
            values = append(values, value)
        }
        series[0].Values = values
    }
    rbody, err = ResultSetBytesFromSeriesAndError(series, rerr)
    return
}

func (iqe *InfluxQLExecutor) reduceBySeries(bodies [][]byte, rerr error) (rbody []byte, err error) {
    var series []*models.Row
    seriesMap := make(map[string]*models.Row)
    for _, body := range bodies {
        _series, _err := SeriesFromResultSetBytes(body)
        if _err != nil {
            err = _err
            return
        }
        for _, serie := range _series {
            if serie.Name != StatisticsMetricName {
                seriesMap[serie.Name] = serie
            }
        }
    }
    for _, serie := range seriesMap {
        series = append(series, serie)
    }
    rbody, err = ResultSetBytesFromSeriesAndError(series, rerr)
    return
}
