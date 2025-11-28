package mgorm

import (
	"errors"

	"go.mongodb.org/mongo-driver/mongo"
)

var ErrConnectionNotInitialized = errors.New("connection not initialized")

func IsUniqIdxConflictError(err error) bool {
	if err != nil {
		var bulkWriteErr mongo.BulkWriteException
		if errors.As(err, &bulkWriteErr) {
			for _, e := range bulkWriteErr.WriteErrors {
				if e.Code != 11000 && e.Code != 11001 && e.Code != 12582 {
					return true
				}
			}
		} else {
			var writeErr mongo.WriteException
			if errors.As(err, &writeErr) {
				if len(writeErr.WriteErrors) > 0 {
					e := writeErr.WriteErrors[0]
					if e.Code == 11000 || e.Code == 11001 || e.Code == 12582 {
						return true
					}
				}
			}
		}
	}
	return false
}
