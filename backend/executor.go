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

func (iqe *InfluxQLExecutor) Query(w http.ResponseWriter, req *http.Request, tokens []string) (err error) {
    stmt := strings.ToLower(tokens[0])
    if stmt == "show" {
        return iqe.QueryShowQL(w, req, tokens)
    } else if stmt == "create" {
        return iqe.QueryCreateQL(w, req, tokens)
    } else if stmt == "delete" || stmt == "drop" {
        return iqe.QueryDeleteOrDropQL(w, req, tokens)
    }
    WriteResponse(w, req, nil, nil, http.StatusOK)
    return
}

func (iqe *InfluxQLExecutor) QueryShowQL(w http.ResponseWriter, req *http.Request, tokens []string) (err error) {
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
        _header, _status, _body, err := api.QueryResp(req)
        if _status >= http.StatusBadRequest {
            copyHeader(w.Header(), _header)
            w.WriteHeader(_status)
            w.Write(_body)
            return nil
        }
        if err != nil {
            return err
        }
        header = _header
        status = _status
        bodies = append(bodies, _body)
    }

    var rsp *Response
    if len(bodies) == 0 {
        rsp = ResponseFromSeries(nil)
    } else {
        stmt2 := GetHeadStmtFromTokens(tokens, 2)
        stmt3 := GetHeadStmtFromTokens(tokens, 3)
        if stmt2 == "show measurements" || stmt2 == "show series" || stmt2 == "show databases" {
            rsp, err = iqe.reduceByValues(bodies)
        } else if stmt3 == "show field keys" || stmt3 == "show tag keys" || stmt3 == "show tag values" {
            rsp, err = iqe.reduceBySeries(bodies)
        } else if stmt3 == "show retention policies" {
            rsp, err = iqe.concatByValues(bodies)
        } else if stmt2 == "show stats" {
            rsp, err = iqe.concatByResults(bodies)
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

func (iqe *InfluxQLExecutor) QueryCreateQL(w http.ResponseWriter, req *http.Request, tokens []string) (err error) {
    reqBodyBytes := ReadBodyBytes(req)
    rsp := ResponseFromSeries(nil)
    var header http.Header
    var inactive int
    if GetHeadStmtFromTokens(tokens, 2) == "create database" {
        for _, api := range iqe.ic.backends {
            if !api.IsActive() {
                inactive++
                continue
            }
            hb := api.(*Backends)
            req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
            req.Form.Del("db")
            if len(tokens) >= 3 {
                tokens[2] = hb.DB
                req.Form.Set("q", strings.Join(tokens, " "))
            } else {
                req.Form.Set("q", "create database " + hb.DB)
            }
            _header, _, _, err := api.QueryResp(req)
            if err != nil {
                return err
            }
            header = _header
        }
        if inactive > 0 {
            rsp.Err = fmt.Sprintf("%d/%d backends not active", inactive, len(iqe.ic.backends))
        }
    }
    WriteResponse(w, req, rsp, header, http.StatusOK)
    return
}

func (iqe *InfluxQLExecutor) QueryDeleteOrDropQL(w http.ResponseWriter, req *http.Request, tokens []string) (err error) {
    reqBodyBytes := ReadBodyBytes(req)
    rsp := ResponseFromSeries(nil)
    var header http.Header
    var inactive int
    if CheckDeleteOrDropMeasurementFromTokens(tokens) {
        key, err := GetMeasurementFromTokens(tokens)
        if err != nil {
            return errors.New("can't get measurement: "+req.FormValue("q"))
        }
        apis, ok := iqe.ic.GetBackends(key)
        if !ok {
            return errors.New(fmt.Sprintf("unknown measurement: %s, query: %s", key, req.FormValue("q")))
        }
        for _, api := range apis {
            if !api.IsActive() {
                inactive++
                continue
            }
            req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
            _header, _, _, err := api.QueryResp(req)
            if err != nil {
                return err
            }
            header = _header
        }
        if inactive > 0 {
            rsp.Err = fmt.Sprintf("%d/%d backends not active", inactive, len(apis))
        }
    }
    WriteResponse(w, req, rsp, header, http.StatusOK)
    return
}

func (iqe *InfluxQLExecutor) reduceByValues(bodies [][]byte) (rsp *Response, err error) {
    var series models.Rows
    var values [][]interface{}
    valuesMap := make(map[string][]interface{})
    for _, b := range bodies {
        _series, err := SeriesFromResponseBytes(b)
        if err != nil {
            return nil, err
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
        _series, err := SeriesFromResponseBytes(b)
        if err != nil {
            return nil, err
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

func (iqe *InfluxQLExecutor) concatByValues(bodies [][]byte) (rsp *Response, err error) {
    var series models.Rows
    var values [][]interface{}
    for _, b := range bodies {
        _series, err := SeriesFromResponseBytes(b)
        if err != nil {
            return nil, err
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

func (iqe *InfluxQLExecutor) concatByResults(bodies [][]byte) (rsp *Response, err error) {
    var results []*Result
    for _, b := range bodies {
        _results, err := ResultsFromResponseBytes(b)
        if err != nil {
            return nil, err
        }
        if len(_results) == 1 {
            results = append(results, _results[0])
        }
    }
    return ResponseFromResults(results), nil
}
