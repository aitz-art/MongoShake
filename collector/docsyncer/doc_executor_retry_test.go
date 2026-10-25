package docsyncer

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

type fakeRetryableErr struct{}

func (fakeRetryableErr) Error() string   { return "pool cleared" }
func (fakeRetryableErr) Retryable() bool { return true }

func TestIsTransientWriteError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", errors.New("boom"), false},
		{"NotWritablePrimary 10107", mongo.CommandError{Code: 10107, Name: "NotWritablePrimary"}, true},
		{"InterruptedDueToReplStateChange 11602", mongo.CommandError{Code: 11602}, true},
		{"wrapped 10107", fmt.Errorf("ctx: %w", mongo.CommandError{Code: 10107}), true},
		{"RetryableWriteError label", mongo.CommandError{Code: 1, Labels: []string{"RetryableWriteError"}}, true},
		{"NetworkError label", mongo.CommandError{Labels: []string{"NetworkError"}}, true},
		{"topology.ConnectionError unlabeled", topology.ConnectionError{ConnectionID: "c1", Wrapped: errors.New("EOF")}, true},
		{"Retryable() true", fakeRetryableErr{}, true},
		{"wrapped Retryable() true", fmt.Errorf("x: %w", fakeRetryableErr{}), true},
		{"BWE only WCE 91", mongo.BulkWriteException{WriteConcernError: &mongo.WriteConcernError{Code: 91}}, true},
		{"BWE only WCE 79", mongo.BulkWriteException{WriteConcernError: &mongo.WriteConcernError{Code: 79}}, false},
		{"BWE only WCE 100", mongo.BulkWriteException{WriteConcernError: &mongo.WriteConcernError{Code: 100}}, false},
		{"dup key 11000", mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 11000}}}}, false},
		{"doc validation 121", mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 121}}}}, false},
		{"CommandError 11000", mongo.CommandError{Code: 11000}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isTransientWriteError(tt.err))
		})
	}
}

func rawDoc(t *testing.T, v interface{}) bson.Raw {
	b, err := bson.Marshal(v)
	assert.NoError(t, err)
	return b
}

func TestIsIDDupKey(t *testing.T) {
	idMsg := "E11000 duplicate key error collection: db.c index: _id_ dup key: { _id: 1 }"
	tests := []struct {
		name string
		we   mongo.WriteError
		want bool
	}{
		{"keyPattern _id", mongo.WriteError{Code: 11000, Raw: rawDoc(t, bson.D{{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}}})}, true},
		{"no keyPattern, _id_ msg", mongo.WriteError{Code: 11000, Message: idMsg}, true},
		{"keyPattern email", mongo.WriteError{Code: 11000, Message: idMsg, Raw: rawDoc(t, bson.D{{Key: "keyPattern", Value: bson.D{{Key: "email", Value: 1}}}})}, false},
		{"compound _id,x", mongo.WriteError{Code: 11000, Raw: rawDoc(t, bson.D{{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}, {Key: "x", Value: 1}}}})}, false},
		{"msg index _id_1", mongo.WriteError{Code: 11000, Message: "E11000 duplicate key error collection: db.c index: _id_1 dup key: { _id: 1 }"}, false},
		{"code 10107", mongo.WriteError{Code: 10107, Message: idMsg}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isIDDupKey(tt.we))
		})
	}
}

func TestFilterRetryDupErrors(t *testing.T) {
	idDup := mongo.BulkWriteError{WriteError: mongo.WriteError{Index: 0, Code: 11000,
		Raw: rawDoc(t, bson.D{{Key: "keyPattern", Value: bson.D{{Key: "_id", Value: 1}}}})}}
	otherDup := mongo.BulkWriteError{WriteError: mongo.WriteError{Index: 1, Code: 11000,
		Raw: rawDoc(t, bson.D{{Key: "keyPattern", Value: bson.D{{Key: "email", Value: 1}}}})}}
	stepdown := mongo.BulkWriteError{WriteError: mongo.WriteError{Index: 2, Code: 11602}}

	tests := []struct {
		name      string
		in        []mongo.BulkWriteError
		allowSkip bool
		want      []mongo.BulkWriteError
	}{
		{"_id dup + 11602 mix", []mongo.BulkWriteError{idDup, stepdown}, true, []mongo.BulkWriteError{stepdown}},
		{"only _id dups, allowSkip", []mongo.BulkWriteError{idDup, idDup}, true, nil},
		{"only _id dups, no allowSkip", []mongo.BulkWriteError{idDup, idDup}, false, []mongo.BulkWriteError{idDup, idDup}},
		{"non-_id dup remains", []mongo.BulkWriteError{idDup, otherDup}, true, []mongo.BulkWriteError{otherDup}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, filterRetryDupErrors(tt.in, tt.allowSkip))
		})
	}
}
