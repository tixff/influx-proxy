// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

var (
    ForbidCmds   = "(?i:^\\s*grant|^\\s*revoke|\\(\\)\\$)"
    SupportCmds  = "(?i:from|drop\\s+measurement)"
    ExecutorCmds = "(?i:create\\s+database$|show\\s+databases$|show\\s+series|show\\s+measurements|show\\s+tag\\s+keys|show\\s+tag\\s+values|show\\s+field\\s+keys|show\\s+retention\\s+policies)"
)
