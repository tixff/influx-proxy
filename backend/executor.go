// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: ping.liu

package backend

import (
    "net/http"
)

type InfluxQLExecutor struct {
}

func (iqe *InfluxQLExecutor) Query(w http.ResponseWriter, req *http.Request) (err error) {
    // q := strings.TrimSpace(req.FormValue("q"))

    w.WriteHeader(200)
    w.Write([]byte(""))

    return
}
