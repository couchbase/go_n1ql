//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package go_n1ql

type n1qlResult struct {
	affectedRows int64
	insertId     int64
}

func (res *n1qlResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

func (res *n1qlResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
