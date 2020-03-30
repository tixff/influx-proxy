// Copyright 2018 BizSeer. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: corylanou, chengshiwen

package backend

import (
    "bytes"
    "compress/gzip"
    "fmt"
    "io"
    "io/ioutil"
    "net/http"
    "strings"

    "github.com/influxdata/influxdb1-client/models"
)

type InfluxQLExecutor struct {
    ic *InfluxCluster
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

func ReadBodyBytes(req *http.Request) (bodyBytes []byte) {
    if req.Body != nil {
        bodyBytes, _ = ioutil.ReadAll(req.Body)
    }
    return
}

func WriteResponse(w http.ResponseWriter, req *http.Request, rsp *Response, header http.Header, status int) {
    copyHeader(w.Header(), header)
    if status > 0 {
        w.WriteHeader(status)
    }
    if rsp == nil {
        rsp = ResponseFromSeries(nil)
    }
    pretty := req.FormValue("pretty") == "true"
    body := rsp.Marshal(pretty)
    if header.Get("Content-Encoding") == "gzip" {
        gzipBody, _ := GzipCompress(body)
        w.Write(gzipBody)
    } else {
        w.Write(body)
    }
}

func (iqe *InfluxQLExecutor) Query(w http.ResponseWriter, req *http.Request) (err error) {
    q := strings.ToLower(strings.TrimSpace(req.FormValue("q")))
    if strings.HasPrefix(q, "show") {
        return iqe.QueryShowQL(w, req)
    } else if strings.HasPrefix(q, "create") {
        return iqe.QueryCreateQL(w, req)
    }
    WriteResponse(w, req, nil, nil, http.StatusOK)
    return
}

func (iqe *InfluxQLExecutor) QueryShowQL(w http.ResponseWriter, req *http.Request) (err error) {
    // remove support of query parameter `chunked`
    req.Form.Del("chunked")
    reqBodyBytes := ReadBodyBytes(req)
    var header http.Header
    var status int
    var bodies [][]byte
    var inactive int
    for _, api := range iqe.ic.backends {
        if api.IsWriteOnly() {
            continue
        }
        if !api.IsActive() {
            inactive++
            continue
        }
        req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
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

    var rsp *Response
    if len(bodies) == 0 {
        rsp = ResponseFromSeries(nil)
    } else {
        q := strings.TrimSpace(req.FormValue("q"))
        if iqe.ic.ExecutedQuery[0].MatchString(q) {
            rsp, err = iqe.reduceByValues(bodies)
        } else if iqe.ic.ExecutedQuery[1].MatchString(q) {
            rsp, err = iqe.reduceBySeries(bodies)
        } else if iqe.ic.ExecutedQuery[2].MatchString(q) {
            rsp, err = iqe.concatByResults(bodies)
        } else if iqe.ic.ExecutedQuery[3].MatchString(q) {
            rsp, err = iqe.concatByValues(bodies)
        } else {
            rsp = ResponseFromSeries(nil)
        }
    }
    if err != nil {
        return
    }
    if inactive > 0 {
        rsp.Err = fmt.Sprintf("%d/%d backends not active", inactive, inactive + len(bodies))
    }
    WriteResponse(w, req, rsp, header, status)
    return
}

func (iqe *InfluxQLExecutor) QueryCreateQL(w http.ResponseWriter, req *http.Request) (err error) {
    q := strings.TrimSpace(req.FormValue("q"))
    reqBodyBytes := ReadBodyBytes(req)
    var header http.Header
    var inactive int
    if iqe.ic.ExecutedQuery[4].MatchString(q) {
        for _, api := range iqe.ic.backends {
            if !api.IsActive() {
                inactive++
                continue
            }
            hb := api.(*Backends)
            req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
            req.Form.Del("db")
            req.Form.Set("q", "create database " + hb.DB)
            _header, _, _, _err := api.QueryResp(req)
            if _err != nil {
                err = _err
                return
            }
            header = _header
        }
    }
    rsp := ResponseFromSeries(nil)
    if inactive > 0 {
        rsp.Err = fmt.Sprintf("%d/%d backends not active", inactive, len(iqe.ic.backends))
    }
    WriteResponse(w, req, rsp, header, http.StatusOK)
    return
}

func (iqe *InfluxQLExecutor) reduceByValues(bodies [][]byte) (rsp *Response, err error) {
    var series models.Rows
    var values [][]interface{}
    valuesMap := make(map[string][]interface{})
    for _, b := range bodies {
        _series, _err := SeriesFromResponseBytes(b)
        if _err != nil {
            err = _err
            return
        }
        if len(_series) == 1 {
            series = _series
            for _, value := range _series[0].Values {
                key := value[0].(string)
                if !strings.HasPrefix(key, StatisticsMeasurementName) {
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
    return ResponseFromSeries(series), nil
}

func (iqe *InfluxQLExecutor) reduceBySeries(bodies [][]byte) (rsp *Response, err error) {
    var series models.Rows
    seriesMap := make(map[string]*models.Row)
    for _, b := range bodies {
        _series, _err := SeriesFromResponseBytes(b)
        if _err != nil {
            err = _err
            return
        }
        for _, serie := range _series {
            if serie.Name != StatisticsMeasurementName {
                seriesMap[serie.Name] = serie
            }
        }
    }
    for _, serie := range seriesMap {
        series = append(series, serie)
    }
    return ResponseFromSeries(series), nil
}

func (iqe *InfluxQLExecutor) concatByResults(bodies [][]byte) (rsp *Response, err error) {
    var results []*Result
    for _, b := range bodies {
        _results, _err := ResultsFromResponseBytes(b)
        if _err != nil {
            err = _err
            return
        }
        if len(_results) == 1 {
            results = append(results, _results[0])
        }
    }
    return ResponseFromResults(results), nil
}

func (iqe *InfluxQLExecutor) concatByValues(bodies [][]byte) (rsp *Response, err error) {
    var series models.Rows
    var values [][]interface{}
    for _, b := range bodies {
        _series, _err := SeriesFromResponseBytes(b)
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
    return ResponseFromSeries(series), nil
}
