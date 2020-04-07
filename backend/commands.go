// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

var (
    SupportCmds = map[string]bool{
        "show measurements": true,
        "show series": true,
        "show field keys": true,
        "show tag keys": true,
        "show tag values": true,
        "show retention policies": true,
        "show stats": true,
        "show databases": true,
        "create database": true,
        "delete from": true,
        "drop series from": true,
        "drop measurement": true,
    }
)
