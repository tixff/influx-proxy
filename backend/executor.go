// Copyright 2018 BizSeer. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: corylanou, chengshiwen

package backend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/influxdata/influxdb1-client/models"
	gzip "github.com/klauspost/pgzip"
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

func Write(w http.ResponseWriter, body []byte, gzip bool) {
	if gzip {
		gzipBody, _ := GzipCompress(body)
		w.Write(gzipBody)
	} else {
		w.Write(body)
	}
}

func WriteBody(w http.ResponseWriter, body []byte, header http.Header, status int) {
	CopyHeader(w.Header(), header)
	w.Header().Del("Content-Length")
	w.WriteHeader(status)
	Write(w, body, header.Get("Content-Encoding") == "gzip")
}

func WriteResp(w http.ResponseWriter, req *http.Request, rsp *Response, header http.Header, status int) {
	CopyHeader(w.Header(), header)
	w.Header().Del("Content-Length")
	if status > 0 {
		w.WriteHeader(status)
	}
	if rsp == nil {
		rsp = ResponseFromSeries(nil)
	}
	pretty := req.FormValue("pretty") == "true"
	body := rsp.Marshal(pretty)
	Write(w, body, header.Get("Content-Encoding") == "gzip")
}

func querySink(api BackendAPI, req http.Request, ch chan *QueryResult, wg *sync.WaitGroup) {
	defer wg.Done()
	req.Body = ioutil.NopCloser(&bytes.Buffer{})
	req.Form = CloneForm(req.Form)
	qr := api.QuerySink(&req)
	ch <- qr
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
	WriteResp(w, req, nil, nil, http.StatusOK)
	return
}

func (iqe *InfluxQLExecutor) QueryShowQL(w http.ResponseWriter, req *http.Request, tokens []string) (err error) {
	// remove support of query parameter `chunked`
	req.Form.Del("chunked")
	var header http.Header
	var status int
	var bodies [][]byte
	var inactive int
	ch := make(chan *QueryResult, len(iqe.ic.backends))
	wg := &sync.WaitGroup{}
	for _, api := range iqe.ic.backends {
		if api.IsWriteOnly() {
			continue
		}
		if !api.IsActive() {
			inactive++
			continue
		}
		wg.Add(1)
		go querySink(api, *req, ch, wg)
	}
	wg.Wait()
	close(ch)
	for qr := range ch {
		if qr.Status >= 400 {
			rsp, _ := ResponseFromResponseBytes(qr.Body)
			if rsp.Err != "" {
				return errors.New(rsp.Err)
			}
			WriteBody(w, qr.Body, qr.Header, qr.Status)
			return
		}
		if qr.Err != nil {
			return qr.Err
		}
		header = qr.Header
		status = qr.Status
		bodies = append(bodies, qr.Body)
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
		rsp.Err = fmt.Sprintf("%d/%d backends unavailable", inactive, inactive+len(bodies))
	}
	WriteResp(w, req, rsp, header, status)
	return
}

func (iqe *InfluxQLExecutor) QueryCreateQL(w http.ResponseWriter, req *http.Request, tokens []string) (err error) {
	rsp := ResponseFromSeries(nil)
	var header http.Header
	var inactive int
	if GetHeadStmtFromTokens(tokens, 2) == "create database" {
		ch := make(chan *QueryResult, len(iqe.ic.backends))
		wg := &sync.WaitGroup{}
		for _, api := range iqe.ic.backends {
			if !api.IsActive() {
				inactive++
				continue
			}
			hb := api.(*Backends)
			req.Form.Del("db")
			if len(tokens) >= 3 {
				tokens[2] = hb.DB
				req.Form.Set("q", strings.Join(tokens, " "))
			} else {
				req.Form.Set("q", "create database "+hb.DB)
			}
			wg.Add(1)
			go querySink(api, *req, ch, wg)
		}
		wg.Wait()
		close(ch)
		for qr := range ch {
			if qr.Err != nil {
				return qr.Err
			}
			header = qr.Header
		}
		if inactive > 0 {
			rsp.Err = fmt.Sprintf("%d/%d backends unavailable", inactive, len(iqe.ic.backends))
		}
	}
	WriteResp(w, req, rsp, header, http.StatusOK)
	return
}

func (iqe *InfluxQLExecutor) QueryDeleteOrDropQL(w http.ResponseWriter, req *http.Request, tokens []string) (err error) {
	rsp := ResponseFromSeries(nil)
	var header http.Header
	var inactive int
	if CheckDeleteOrDropMeasurementFromTokens(tokens) {
		key, err := GetMeasurementFromTokens(tokens)
		if err != nil {
			return ErrGetMeasurement
		}
		apis, ok := iqe.ic.GetBackends(key)
		if !ok {
			return ErrUnknownMeasurement
		}
		ch := make(chan *QueryResult, len(iqe.ic.backends))
		wg := &sync.WaitGroup{}
		for _, api := range apis {
			if !api.IsActive() {
				inactive++
				continue
			}
			wg.Add(1)
			go querySink(api, *req, ch, wg)
		}
		wg.Wait()
		close(ch)
		for qr := range ch {
			if qr.Err != nil {
				return qr.Err
			}
			header = qr.Header
		}
		if inactive > 0 {
			rsp.Err = fmt.Sprintf("%d/%d backends unavailable", inactive, len(apis))
		}
	} else {
		return ErrIllegalQL
	}
	WriteResp(w, req, rsp, header, http.StatusOK)
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
		if len(values) > 0 {
			series[0].Values = values
		} else {
			series = nil
		}
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
			values = append(values, _series[0].Values...)
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
