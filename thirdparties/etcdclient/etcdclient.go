package main

/*
#include <stdlib.h>

enum EtcdErrCode
{
  StatusOK = 0,
  ErrNewEtcdClientV3 = -1,
  ErrEtcdPut = -2,
  ErrEtcdGet = -3,
  ErrEtcdGetNotExist = -4,
  ErrEtcdList = -5,
  ErrEtcdListNotExist = -6,
  ErrEtcdDelete = -7,
  ErrEtcdRename = -8,
  ErrEtcdSnapshot = -9,
  ErrObjectNotExist = -10,
  ErrObjectType = -11,
  ErrEtcdTxn = -12,
  ErrEtcdTxnUnkownOp = -13
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
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	mvccpb "go.etcd.io/etcd/mvcc/mvccpb"
	"strings"
	"time"
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

// TODO(lixiaocui): 下面的操作err细分，可能需要增加重试逻辑；
// TODO(lixiaocui): 日志打印看是否需要glog
//export NewEtcdClientV3
func NewEtcdClientV3(conf C.struct_EtcdConf) C.enum_EtcdErrCode {
	var err error
	globalClient, err = clientv3.New(clientv3.Config{
		Endpoints:   GetEndpoint(C.GoStringN(conf.Endpoints, conf.len)),
		DialTimeout: time.Duration(int(conf.DialTimeout)) * time.MilliSecond,
	})
	if err != nil {
		fmt.Printf("new etcd client err: %v\n", err)
		return C.ErrNewEtcdClientV3
	}
	return C.StatusOK
}

//export EtcdClientPut
func EtcdClientPut(timeout C.int, key, value *C.char, 
	keyLen, valueLen C.int) C.enum_EtcdErrCode {
	goKey, goValue := C.GoStringN(key, keyLen), C.GoStringN(value, valueLen)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout))*time.MilliSecond)
	defer cancel()

	_, err := globalClient.Put(ctx, goKey, goValue)
	if err != nil {
		switch err {
		case context.Canceled:
			fmt.Printf("ctx is cancled by other routine: %v\n", err)
		case context.DeadlineExceeded:
			fmt.Printf("ctx is attached with a deadline " +
				"is exceeded: %v\n", err)
		case rpctypes.ErrEmptyKey:
			fmt.Printf("client-side error: %v\n", err)
		default:
			fmt.Printf("bad cluster endpoints, which are not " +
				"etcd servers: %v\n", err)
		}
		return C.ErrEtcdPut
	}
	return C.StatusOK
}

//export EtcdClientGet
func EtcdClientGet(timeout C.int, key *C.char, keyLen C.int)
	(C.enum_EtcdErrCode, *C.char, int) {
	goKey := C.GoStringN(key, keyLen)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout))*time.MilliSecond)
	defer cancel()

	resp, err := globalClient.Get(ctx, goKey)
	if err != nil {
		fmt.Printf("get key:%v err:%v\n", goKey, err)
		return C.ErrEtcdGet, nil
	}

	if resp.Count <= 0 {
		fmt.Printf("key:%v do not exist\n", goKey)
		return C.ErrEtcdGetNotExist, nil
	}

	return C.StatusOK,
		   C.CString(string(resp.Kvs[0].Value)),
		   len(resp[0].Kvs[0].Value)
}

// TODO(lixiaocui): list可能需要有长度限制
//export EtcdClientList
func EtcdClientList(timeout C.int, startKey, endKey *C.char,
	startLen, endLen C.int) (C.enum_EtcdErrCode, uint64, int64) {
	goStartKey := C.GoStringN(startKey, startLen)
	goEndKey := C.GoStringN(endKey, endLen)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout)) * time.MilliSecond)
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

	if err != nil {
		fmt.Printf("list [startKey:%v, endKey:%v) err:%v",
					goStartKey, goEndKey, err)
		return C.ErrEtcdList, 0, 0
	}

	if resp.Count <= 0 {
		fmt.Printf("list [startKey:%v, endKey:%v) not exist",
					goStartKey, goEndKey)
		return C.ErrEtcdListNotExist, 0, 0
	}

	return  C.StatusOK, AddManagedObject(resp.Kvs), resp.Count
}

//export EtcdClientDelete
func EtcdClientDelete(
	timeout C.int, key *C.char, keyLen C.int) C.enum_EtcdErrCode {
	goKey := C.GoStringN(key, keyLen)
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout))*time.MilliSecond)
	defer cancel()

	dresp, err := globalClient.Delete(ctx, goKey)
	if err != nil {
		fmt.Printf("delete key:%v err:%v\n", goKey, err)
		return C.ErrEtcdDelete
	}

	fmt.Printf("delete %d entries of key:%v\n", dresp.Deleted, goKey)

	return C.StatusOK
}

//export EtcdClientTxn2
func EtcdClientTxn2(
	timeout C.int, op1, op2 C.struct_Operation) C.enum_EtcdErrCode {
	ops := []C.struct_Operation{op1, op2}
	etcdOps, err := GenOpList(ops)
	if err != nil {
		fmt.Printf("unknown op types, err: %v", err)
		return C.ErrEtcdTxnUnkownOp
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(int(timeout))*time.MilliSecond)
	defer cancel()

	_, err = globalClient.Txn(ctx).Then(etcdOps...).Commit()
	if err != nil {
		fmt.Printf("etcd do txn(%+v) err: %+v", ops, err)
		return C.ErrEtcdTxn
	}

	return C.StatusOK
}

//export EtcdClientGetSingleObject
func EtcdClientGetSingleObject(
	oid uint64) (C.enum_EtcdErrCode, *C.char, int) {
	if value, exist := GetManagedObject(oid); !exist {
		fmt.Printf("can not get object: %v", oid)
		return C.ErrObjectNotExist, nil, 0
	} else if res, ok := value.([]*mvccpb.KeyValue); ok{
		return C.StatusOK, C.CString(string(res[0].Value)), len(res[0].Value)
	} else {
		fmt.Printf("object type err")
		return C.ErrObjectType, nil, 0
	}
}

//export EtcdClientGetMultiObject
func EtcdClientGetMultiObject(
	oid uint64, serial int) (C.enum_EtcdErrCode, *C.char, int) {
	if value, exist := GetManagedObject(oid); !exist {
		return C.ErrObjectNotExist, nil, 0
	} else if res, ok := value.([]*mvccpb.KeyValue); ok {
		return C.StatusOK, C.CString(string(res[serial].Value)),
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
