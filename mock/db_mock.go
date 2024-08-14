// Code generated by MockGen. DO NOT EDIT.
// Source: ./db/types.go
//
// Generated by this command:
//
//	mockgen -package mock -destination mock/db_mock.go -source ./db/types.go DB
//

// Package mock is a generated GoMock package.
package mock

import (
	reflect "reflect"

	store "cosmossdk.io/core/store"
	gomock "go.uber.org/mock/gomock"
)

// MockDB is a mock of DB interface.
type MockDB struct {
	ctrl     *gomock.Controller
	recorder *MockDBMockRecorder
}

// MockDBMockRecorder is the mock recorder for MockDB.
type MockDBMockRecorder struct {
	mock *MockDB
}

// NewMockDB creates a new mock instance.
func NewMockDB(ctrl *gomock.Controller) *MockDB {
	mock := &MockDB{ctrl: ctrl}
	mock.recorder = &MockDBMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDB) EXPECT() *MockDBMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockDB) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockDBMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockDB)(nil).Close))
}

// Delete mocks base method.
func (m *MockDB) Delete(key []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", key)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockDBMockRecorder) Delete(key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockDB)(nil).Delete), key)
}

// Get mocks base method.
func (m *MockDB) Get(arg0 []byte) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", arg0)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockDBMockRecorder) Get(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockDB)(nil).Get), arg0)
}

// Has mocks base method.
func (m *MockDB) Has(key []byte) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Has", key)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Has indicates an expected call of Has.
func (mr *MockDBMockRecorder) Has(key any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Has", reflect.TypeOf((*MockDB)(nil).Has), key)
}

// Iterator mocks base method.
func (m *MockDB) Iterator(start, end []byte) (store.Iterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Iterator", start, end)
	ret0, _ := ret[0].(store.Iterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Iterator indicates an expected call of Iterator.
func (mr *MockDBMockRecorder) Iterator(start, end any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Iterator", reflect.TypeOf((*MockDB)(nil).Iterator), start, end)
}

// NewBatch mocks base method.
func (m *MockDB) NewBatch() store.Batch {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewBatch")
	ret0, _ := ret[0].(store.Batch)
	return ret0
}

// NewBatch indicates an expected call of NewBatch.
func (mr *MockDBMockRecorder) NewBatch() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewBatch", reflect.TypeOf((*MockDB)(nil).NewBatch))
}

// NewBatchWithSize mocks base method.
func (m *MockDB) NewBatchWithSize(arg0 int) store.Batch {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewBatchWithSize", arg0)
	ret0, _ := ret[0].(store.Batch)
	return ret0
}

// NewBatchWithSize indicates an expected call of NewBatchWithSize.
func (mr *MockDBMockRecorder) NewBatchWithSize(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewBatchWithSize", reflect.TypeOf((*MockDB)(nil).NewBatchWithSize), arg0)
}

// ReverseIterator mocks base method.
func (m *MockDB) ReverseIterator(start, end []byte) (store.Iterator, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReverseIterator", start, end)
	ret0, _ := ret[0].(store.Iterator)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReverseIterator indicates an expected call of ReverseIterator.
func (mr *MockDBMockRecorder) ReverseIterator(start, end any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReverseIterator", reflect.TypeOf((*MockDB)(nil).ReverseIterator), start, end)
}

// Set mocks base method.
func (m *MockDB) Set(key, value []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Set", key, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// Set indicates an expected call of Set.
func (mr *MockDBMockRecorder) Set(key, value any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Set", reflect.TypeOf((*MockDB)(nil).Set), key, value)
}
