// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: ping.liu, chengshiwen

package backend

import (
    "encoding/json"
    "errors"

    "github.com/influxdata/influxdb1-client/models"
)

// Message represents a user-facing message to be included with the result.
type Message struct {
    Level string `json:"level"`
    Text  string `json:"text"`
}

// Result represents a resultset returned from a single statement.
// Rows represents a list of rows that can be sorted consistently by name/tag.
type Result struct {
    // StatementID is just the statement's position in the query. It's used
    // to combine statement results if they're being buffered in memory.
    StatementID int
    Series      models.Rows
    Messages    []*Message
    Partial     bool
    Err         error
}

// MarshalJSON encodes the result into JSON.
func (r *Result) MarshalJSON() ([]byte, error) {
    // Define a struct that outputs "error" as a string.
    var o struct {
        StatementID int           `json:"statement_id"`
        Series      []*models.Row `json:"series,omitempty"`
        Messages    []*Message    `json:"messages,omitempty"`
        Partial     bool          `json:"partial,omitempty"`
        Err         string        `json:"error,omitempty"`
    }

    // Copy fields to output struct.
    o.StatementID = r.StatementID
    o.Series = r.Series
    o.Messages = r.Messages
    o.Partial = r.Partial
    if r.Err != nil {
        o.Err = r.Err.Error()
    }

    return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Result struct
func (r *Result) UnmarshalJSON(b []byte) error {
    var o struct {
        StatementID int           `json:"statement_id"`
        Series      []*models.Row `json:"series,omitempty"`
        Messages    []*Message    `json:"messages,omitempty"`
        Partial     bool          `json:"partial,omitempty"`
        Err         string        `json:"error,omitempty"`
    }

    err := json.Unmarshal(b, &o)
    if err != nil {
        return err
    }
    r.StatementID = o.StatementID
    r.Series = o.Series
    r.Messages = o.Messages
    r.Partial = o.Partial
    if o.Err != "" {
        r.Err = errors.New(o.Err)
    }
    return nil
}

// ResultSet represent multiple resultsets returned from multiple statements.
type ResultSet struct {
    Results []Result
}

// MarshalJSON encodes the ResultSet into JSON.
func (rs *ResultSet) MarshalJSON() ([]byte, error) {
    // Define a struct that outputs "error" as a string.
    var o struct {
        Results []Result `json:"results"`
    }

    // Copy fields to output struct.
    o.Results = rs.Results
    return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the ResultSet struct
func (rs *ResultSet) UnmarshalJSON(b []byte) error {
    var o struct {
        Results []Result `json:"results"`
    }

    err := json.Unmarshal(b, &o)
    if err != nil {
        return err
    }
    rs.Results = o.Results
    return nil
}

// TODO: multi queries in q?
func SeriesFromResultSetBytes(b []byte) (series []*models.Row, err error) {
    rs := &ResultSet{}
    err = rs.UnmarshalJSON(b)
    if err == nil && len(rs.Results) > 0 && len(rs.Results[0].Series) > 0 {
        series = rs.Results[0].Series
    }
    return
}

func ResultSetBytesFromSeries(series []*models.Row) (b []byte, err error) {
    r := Result{
        Series: series,
    }
    rs := &ResultSet{
        Results: []Result{r},
    }
    b, err = rs.MarshalJSON()
    if err == nil {
        b = append(b, '\n')
    }
    return
}
