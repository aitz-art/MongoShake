package executor

import (
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/mongo"
)

func TestIsStateChangeError(t *testing.T) {
	notPrimary := mongo.CommandError{Code: 10107, Name: "NotWritablePrimary", Message: "not primary"}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", errors.New("not primary"), false},
		{"CommandError NotWritablePrimary", notPrimary, true},
		{"CommandError InterruptedDueToReplStateChange", mongo.CommandError{Code: 11602}, true},
		{"CommandError LegacyNotPrimary", mongo.CommandError{Code: 10058}, true},
		{"wrapped %w", fmt.Errorf("delete+insert fallback: %w", notPrimary), true},
		{"wrapped %v loses code", fmt.Errorf("x: %v", notPrimary), false},
		{"WriteException WriteConcernError", mongo.WriteException{
			WriteConcernError: &mongo.WriteConcernError{Code: 10107}}, true},
		{"WriteException WriteError", mongo.WriteException{
			WriteErrors: mongo.WriteErrors{{Code: 10107}}}, true},
		{"BulkWriteException WriteConcernError", mongo.BulkWriteException{
			WriteConcernError: &mongo.WriteConcernError{Code: 91}}, true},
		{"BulkWriteException duplicate key", mongo.BulkWriteException{
			WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 11000}}}}, false},
		{"CommandError duplicate key", mongo.CommandError{Code: 11000}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isStateChangeError(tt.err); got != tt.want {
				t.Errorf("isStateChangeError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
