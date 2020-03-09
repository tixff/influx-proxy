// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

var (
    ForbidCmds   = []string{"(?i:^grant|^revoke|^select.+into.+from)"}
    SupportCmds  = []string{"(?i:from|^drop\\s+measurement)"}
    ExecutorCmds = []string{
        "(?i:^show\\s+measurements|^show\\s+series|^show\\s+databases$)",
        "(?i:^show\\s+field\\s+keys|^show\\s+tag\\s+keys|^show\\s+tag\\s+values)",
        "(?i:^show\\s+retention\\s+policies)",
        "(?i:^create\\s+database)",
    }
)
