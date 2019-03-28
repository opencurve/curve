package main

/*
#include <stdlib.h>

enum EtcdErrCode
{
	// grpc errCode, 具体的含义见:
	// https://godoc.org/go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes#ErrGRPCNoSpace
	// https://godoc.org/google.golang.org/grpc/codes#Code
	OK = 0,
	Canceled,
	Unknown,
	InvalidArgument,
	DeadlineExceeded,
	NotFound,
	AlreadyExists,
	PermissionDenied,
	ResourceExhausted,
	FailedPrecondition,
	Aborted,
	OutOfRange,
	Unimplemented,
	Internal,
	Unavailable,
	DataLoss,
	Unauthenticated,

	// 自定义错误码
	TxnUnkownOp,
	ObjectNotExist,
	ErrObjectType,
	KeyNotExist,
};

enum OpType {
  OpPut = 1,
  OpDelete = 2
};

struct EtcdConf {
	char *Endpoints;
	int len;
    int DialTimeout;
};

struct Operation {
	enum OpType opType;
	char *key;
	char *value;
	int keyLen;
	int valueLen;
};
*/
import "C"
import (
	"context"
	"fmt"
	"errors"
	"strings"
	"time"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	mvccpb "go.etcd.io/etcd/mvcc/mvccpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	EtcdNewClient = "NewCient"
	EtcdPut = "Put"
	EtcdGet = "Get"
	EtcdList = "List"
	EtcdDelete = "Delete"
	EtcdTxn2 = "Txn2"
)

var globalClient *clientv3.Client

func GetEndpoint(endpoints string) []string {
	sub := strings.Split(endpoints, ",")
	res := make([]string, 0, len(sub))
	for _, elem := range sub {
		res = append(res, elem)
	}
	return res
}

func GenOpList(cops []C.struct_Operation) ([]clientv3.Op, error) {
	res := make([]clientv3.Op, 0, len(cops))
	for _, op := range cops {
		switch op.opType {
			case C.OpPut:
				goKey := C.GoStringN(op.key, op.keyLen)
				goValue := C.GoStringN(op.value, op.valueLen)
				res = append(res, clientv3.OpPut(goKey, goValue))
			case C.OpDelete:
				goKey := C.GoStringN(op.key, op.keyLen)
				res = append(res, clientv3.OpDelete(goKey))
			default:
				fmt.Printf("opType:%v do not exist", op.opType)
				return res, errors.New("opType do not exist")
		}
	}
	return res, nil
}

func GetErrCode(op string, err error) C.enum_EtcdErrCode {
	errCode := codes.Unknown
	if entity, ok := err.(rpctypes.EtcdError); ok {
		errCode = entity.Code()
	} else if ev, ok := status.FromError(err); ok {
		errCode = ev.Code()
	} else if err == context.Canceled {
		errCode = codes.Canceled
	} else if err == context.DeadlineExceeded {
		errCode = codes.DeadlineExceeded
	}

	if codes.OK != errCode {
		fmt.Printf("etcd do %v get err:%v, errCode:%v\n", op, err, errCode)
	}

	switch errCode {
	case codes.OK:
		return C.OK
	case codes.Canceled:
		return C.Canceled
	case codes.Unknown:
		return C.Unknown
	case codes.InvalidArgument:
		return C.InvalidArgument
	case codes.DeadlineExceeded:
		return C.DeadlineExceeded
	case codes.NotFound:
		return C.NotFound
	case codes.AlreadyExists:
		return C.AlreadyExists
	case codes.PermissionDenied:
		return C.PermissionDenied
	case codes.ResourceExhausted:
		return C.ResourceExhausted
	case codes.FailedPrecondition:
		return C.FailedPrecondition
	case codes.Aborted:
		return C.Aborted
	case codes.OutOfRange:
		return C.OutOfRange
	case codes.Unimplemented:
		return C.Unimplemented
	case codes.Internal:
		return C.Internal
	case codes.Unavailable:
		return C.Unavailable
	case codes.DataLoss:
		return C.DataLoss
	case codes.Unauthenticated:
		return C.Unauthenticated
	}

	return C.Unknown
}

// TODO(lixiaocui): 日志打印看是否需要glog
//export NewEtcdClientV3
func NewEtcdClientV3(conf C.struct_EtcdConf) C.enum_EtcdErrCode {
	var err error
	globalClient, err = clientv3.New(clientv3.Config{
		Endpoints:   GetEndpoint(C.GoStringN(conf.Endpoints, conf.len)),
		DialTimeout: time.Duration(int(conf.DialTimeout)) * time.Millisecond,
	})
	return GetErrCode(EtcdNewClient, err)
}

//export EtcdCloseClient
func EtcdCloseClient() {
	globalClient.Close()
}

//export EtcdClientPut
func EtcdClientPut(timeout C.int, key, value *C.char,
	keyLen, valueLen C.int) C.enum_EtcdErrCode {
	goKey, goValue := C.GoStringN(key, keyLen), C.GoStringN(value, valueLen)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout))*time.Millisecond)
	defer cancel()

	_, err := globalClient.Put(ctx, goKey, goValue)
	return GetErrCode(EtcdPut, err)
}

//export EtcdClientGet
func EtcdClientGet(timeout C.int, key *C.char,
	keyLen C.int) (C.enum_EtcdErrCode, *C.char, int) {
	goKey := C.GoStringN(key, keyLen)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout))*time.Millisecond)
	defer cancel()

	resp, err := globalClient.Get(ctx, goKey)
	errCode := GetErrCode(EtcdGet, err)
	if errCode != C.OK {
		return errCode, nil, 0
	}

	if resp.Count <= 0 {
		return C.KeyNotExist, nil, 0
	}

	return errCode,
		   C.CString(string(resp.Kvs[0].Value)),
		   len(resp.Kvs[0].Value)
}

// TODO(lixiaocui): list可能需要有长度限制
//export EtcdClientList
func EtcdClientList(timeout C.int, startKey, endKey *C.char,
	startLen, endLen C.int) (C.enum_EtcdErrCode, uint64, int64) {
	goStartKey := C.GoStringN(startKey, startLen)
	goEndKey := C.GoStringN(endKey, endLen)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout)) * time.Millisecond)
	defer cancel()

	var resp *clientv3.GetResponse
	var err error
	if goEndKey == "" {
		// return keys >= start
		resp, err = globalClient.Get(
			ctx, goStartKey, clientv3.WithFromKey());
	} else {
		// return keys in range [start, end)
		resp, err = globalClient.Get(
			ctx, goStartKey, clientv3.WithRange(goEndKey))
	}

	errCode := GetErrCode(EtcdList, err)
	if errCode != C.OK {
		return errCode, 0, 0
	}
	return  errCode, AddManagedObject(resp.Kvs), resp.Count
}

//export EtcdClientDelete
func EtcdClientDelete(timeout C.int, key *C.char, keyLen C.int) C.enum_EtcdErrCode {
	goKey := C.GoStringN(key, keyLen)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout))*time.Millisecond)
	defer cancel()

	_, err := globalClient.Delete(ctx, goKey)
	return GetErrCode(EtcdDelete, err)
}

//export EtcdClientTxn2
func EtcdClientTxn2(timeout C.int, op1, op2 C.struct_Operation) C.enum_EtcdErrCode {
	ops := []C.struct_Operation{op1, op2}
	etcdOps, err := GenOpList(ops)
	if err != nil {
		fmt.Printf("unknown op types, err: %v", err)
		return C.TxnUnkownOp
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout))*time.Millisecond)
	defer cancel()

	_, err = globalClient.Txn(ctx).Then(etcdOps...).Commit()
	return GetErrCode(EtcdTxn2, err)
}

//export EtcdClientGetSingleObject
func EtcdClientGetSingleObject(
	oid uint64) (C.enum_EtcdErrCode, *C.char, int) {
	if value, exist := GetManagedObject(oid); !exist {
		fmt.Printf("can not get object: %v", oid)
		return C.ObjectNotExist, nil, 0
	} else if res, ok := value.([]*mvccpb.KeyValue); ok {
		return C.OK, C.CString(string(res[0].Value)), len(res[0].Value)
	} else {
		fmt.Printf("object type err")
		return C.ErrObjectType, nil, 0
	}
}

//export EtcdClientGetMultiObject
func EtcdClientGetMultiObject(
	oid uint64, serial int) (C.enum_EtcdErrCode, *C.char, int) {
	if value, exist := GetManagedObject(oid); !exist {
		return C.ObjectNotExist, nil, 0
	} else if res, ok := value.([]*mvccpb.KeyValue); ok {
		return C.OK, C.CString(string(res[serial].Value)),
			   len(res[serial].Value)
	} else {
		return C.ErrObjectType, nil, 0
	}
}

//export EtcdClientRemoveObject
func EtcdClientRemoveObject(oid uint64) {
	RemoveManagedObject(oid)
}

func main() {}
