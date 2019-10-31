package main

/*
#include <stdlib.h>

enum EtcdErrCode
{
    // grpc errCode, 具体的含义见:
    // https://godoc.org/go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes#ErrGRPCNoSpace
    // https://godoc.org/google.golang.org/grpc/codes#Code
    OK = 0,
    Canceled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,

    // 自定义错误码
    TxnUnkownOp = 17,
    ObjectNotExist = 18,
    ErrObjectType = 19,
    KeyNotExist = 20,
    CampaignInternalErr = 21,
    CampaignLeaderSuccess = 22,
    ObserverLeaderInternal = 23,
    ObserverLeaderChange = 24,
    LeaderResignErr = 25,
    LeaderResiginSuccess = 26,
    GetLeaderKeyErr = 27,
    GetLeaderKeyOK = 28,
    ObserverLeaderNotExist = 29,
    ObjectLenNotEnough = 30,
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
    "log"
    "errors"
    "strings"
    "time"
    "go.etcd.io/etcd/clientv3"
    "go.etcd.io/etcd/clientv3/concurrency"
    "go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
    mvccpb "go.etcd.io/etcd/mvcc/mvccpb"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/grpc"
)

const (
    EtcdNewClient = "NewCient"
    EtcdPut = "Put"
    EtcdGet = "Get"
    EtcdList = "List"
    EtcdDelete = "Delete"
    EtcdTxn2 = "Txn2"
    EtcdTxn3 = "Txn3"
    EtcdCmpAndSwp = "CmpAndSwp"
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
                log.Printf("opType:%v do not exist", op.opType)
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
        log.Printf("etcd do %v get err:%v, errCode:%v", op, err, errCode)
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
        DialOptions: []grpc.DialOption{grpc.WithBlock()},
    })
    return GetErrCode(EtcdNewClient, err)
}

//export EtcdCloseClient
func EtcdCloseClient() {
    if globalClient != nil {
        globalClient.Close()
    }
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

//export EtcdClientPutRewtihRevision
func EtcdClientPutRewtihRevision(timeout C.int, key, value *C.char,
    keyLen, valueLen C.int) (C.enum_EtcdErrCode, int64) {
    goKey, goValue := C.GoStringN(key, keyLen), C.GoStringN(value, valueLen)
    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(int(timeout))*time.Millisecond)
    defer cancel()

    resp, err := globalClient.Put(ctx, goKey, goValue)

    if err == nil {
        return GetErrCode(EtcdPut, err), resp.Header.Revision
    }
    return GetErrCode(EtcdPut, err), 0
}

//export EtcdClientGet
func EtcdClientGet(timeout C.int, key *C.char,
    keyLen C.int) (C.enum_EtcdErrCode, *C.char, int, int64) {
    goKey := C.GoStringN(key, keyLen)
    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(int(timeout))*time.Millisecond)
    defer cancel()

    resp, err := globalClient.Get(ctx, goKey)
    errCode := GetErrCode(EtcdGet, err)
    if errCode != C.OK {
        return errCode, nil, 0, 0
    }

    if resp.Count <= 0 {
        return C.KeyNotExist, nil, 0, resp.Header.Revision
    }

    return errCode,
           C.CString(string(resp.Kvs[0].Value)),
           len(resp.Kvs[0].Value),
           resp.Header.Revision
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
            ctx, goStartKey, clientv3.WithFromKey())
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

//export EtcdClientListWithLimitAndRevision
func EtcdClientListWithLimitAndRevision(timeout C.uint, startKey, endKey *C.char,
    startLen, endLen C.int, limit int64, startRevision int64)(
    C.enum_EtcdErrCode, uint64, int, int64) {
    goStartKey := C.GoStringN(startKey, startLen)
    goEndKey := C.GoStringN(endKey, endLen)
    ctx, cancle := context.WithTimeout(context.Background(),
        time.Duration(int(timeout)) * time.Millisecond)
    defer cancle()

    var resp *clientv3.GetResponse
    var err error
    ops := []clientv3.OpOption{
        clientv3.WithLimit(limit),
        clientv3.WithRev(startRevision),
        clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)}

    if goEndKey == "" {
        ops = append(ops, clientv3.WithFromKey())
    } else {
        ops = append(ops, clientv3.WithRange(goEndKey))
    }

    resp, err = globalClient.Get(ctx, goStartKey, ops...)
    errCode := GetErrCode(EtcdList, err)
    if errCode != C.OK {
        return errCode, 0, 0, 0
    }
    return errCode, AddManagedObject(resp.Kvs), len(resp.Kvs), resp.Header.Revision
}

//export EtcdClientDelete
func EtcdClientDelete(
    timeout C.int, key *C.char, keyLen C.int) C.enum_EtcdErrCode {
    goKey := C.GoStringN(key, keyLen)
    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(int(timeout))*time.Millisecond)
    defer cancel()

    _, err := globalClient.Delete(ctx, goKey)
    return GetErrCode(EtcdDelete, err)
}

//export EtcdClientDeleteRewithRevision
func EtcdClientDeleteRewithRevision(
    timeout C.int, key *C.char, keyLen C.int) (C.enum_EtcdErrCode, int64) {
    goKey := C.GoStringN(key, keyLen)
    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(int(timeout))*time.Millisecond)
    defer cancel()

    resp, err := globalClient.Delete(ctx, goKey)
    if err == nil {
        return GetErrCode(EtcdDelete, err), resp.Header.Revision
    }
    return GetErrCode(EtcdDelete, err), 0
}

//export EtcdClientTxn2
func EtcdClientTxn2(
    timeout C.int, op1, op2 C.struct_Operation) C.enum_EtcdErrCode {
    ops := []C.struct_Operation{op1, op2}
    etcdOps, err := GenOpList(ops)
    if err != nil {
        log.Printf("unknown op types, err: %v", err)
        return C.TxnUnkownOp
    }

    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(int(timeout))*time.Millisecond)
    defer cancel()

    _, err = globalClient.Txn(ctx).Then(etcdOps...).Commit()
    return GetErrCode(EtcdTxn2, err)
}

//export EtcdClientTxn3
func EtcdClientTxn3(
    timeout C.int, op1, op2, op3 C.struct_Operation) C.enum_EtcdErrCode {
    ops := []C.struct_Operation{op1, op2, op3}
    etcdOps, err := GenOpList(ops)
    if (err != nil) {
        log.Printf("unknown op types, err: %v", err)
        return C.TxnUnkownOp
    }

    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(int(timeout))*time.Millisecond)
    defer cancel()

    _, err = globalClient.Txn(ctx).Then(etcdOps...).Commit()
    return GetErrCode(EtcdTxn3, err)
}

//export EtcdClientCompareAndSwap
func EtcdClientCompareAndSwap(timeout C.int, key, prev, target *C.char,
    keyLen, preLen, targetLen C.int) C.enum_EtcdErrCode {
    goKey := C.GoStringN(key, keyLen)
    goPrev := C.GoStringN(prev, preLen)
    goTarget := C.GoStringN(target, targetLen)

    ctx, cancel := context.WithTimeout(context.Background(),
    time.Duration(int(timeout))*time.Millisecond)
    defer cancel()

    var err error
    _, err = globalClient.Txn(ctx).
        If(clientv3.Compare(clientv3.CreateRevision(goKey), "=", 0)).
        Then(clientv3.OpPut(goKey, goTarget)).
        Commit()

    _, err = globalClient.Txn(ctx).
        If(clientv3.Compare(clientv3.Value(goKey), "=", goPrev)).
        Then(clientv3.OpPut(goKey, goTarget)).
        Commit()
    return GetErrCode(EtcdCmpAndSwp, err)
}

//export EtcdElectionCampaign
func EtcdElectionCampaign(pfx *C.char, pfxLen C.int,
    leaderName *C.char, nameLen C.int, sessionInterSec uint32,
    electionTimeoutMs uint32) (C.enum_EtcdErrCode, uint64) {
    // TODO(lixiaocui):  MDS的切换时间是否能够控制在 ms级别，比如500ms以内
    // 在正常kill的情况下，mds的切换时间内可以控制在ms级别
    goPfx := C.GoStringN(pfx, pfxLen)
    goLeaderName := C.GoStringN(leaderName, nameLen)

    // 创建带ttl的session
    var sessionOpts concurrency.SessionOption =
            concurrency.WithTTL(int(sessionInterSec))
    session, err := concurrency.NewSession(globalClient, sessionOpts)
    if err != nil {
        log.Printf("%v new session err: %v", goLeaderName, err)
        return C.CampaignInternalErr, 0
    }

    // 创建election和超时context
    var election *concurrency.Election = concurrency.NewElection(session, goPfx)
    var ctx context.Context
    var cancel context.CancelFunc
    if electionTimeoutMs > 0 {
        ctx, cancel = context.WithTimeout(context.Background(),
            time.Duration(int(electionTimeoutMs)) * time.Millisecond)
        defer cancel()
    } else {
        ctx = context.Background()
    }

    // 获取当前leader并打印
    if leader, err := election.Leader(ctx); err == nil {
        log.Printf("current leader is: %v", string(leader.Kvs[0].Value))
    } else {
        log.Printf("get current leader err: %v", err)
    }

    // 如果contex是'context.TODO()/context.Background()', 竞选会阻塞到该key被删除,
    // 除非返回non-recoverable error(例如: ErrCompacted).
    // 如果context有超时, 在超时或者取消之前, 竞选会阻塞到当选leader
    log.Printf("%v campagin for leader begin", goLeaderName)
    if err := election.Campaign(ctx, goLeaderName); err != nil {
        log.Printf("%v campaign err: %v", goLeaderName, err)
        session.Close()
        return C.CampaignInternalErr, 0
    } else {
        log.Printf("%v campaign for leader success", goLeaderName)
    }
    return C.CampaignLeaderSuccess, AddManagedObject(election)
}

//export EtcdElectionLeaderKeyExist
func EtcdElectionLeaderKeyExist(
    leaderOid uint64, timeout uint64) C.enum_EtcdErrCode   {
    election := GetLeaderElection(leaderOid)
    if election == nil {
        log.Printf("EtcdElectionLeaderKeyExist can not get leader object: %v",
            leaderOid)
        return C.ObjectNotExist
    }
    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(int(timeout))*time.Millisecond)
    defer cancel()

    resp, err := election.Leader(ctx)
    if err != nil {
        log.Printf("EtcdElectionLeaderKeyExist get leader err: %v", err)
        return C.GetLeaderKeyErr
    } else if string(resp.Kvs[0].Key) != election.Key() {
        log.Printf("EtcdElectionLeaderKeyExist get current " +
            "leader key: %v, self key：%v", resp.Kvs[0].Key, election.Key())
        return C.GetLeaderKeyErr
    }
    return C.GetLeaderKeyOK
}


//export EtcdLeaderObserve
func EtcdLeaderObserve(leaderOid uint64, timeout uint64,
    leaderName *C.char, nameLen C.int) C.enum_EtcdErrCode {
    goLeaderName := C.GoStringN(leaderName, nameLen)

    election := GetLeaderElection(leaderOid)
    if election == nil {
        log.Printf("can not get leader object: %v", leaderOid)
        return C.ObjectNotExist
    }

    log.Printf("start observe: %v", goLeaderName)
    var ctx context.Context
    ctx = clientv3.WithRequireLeader(context.Background())

    ticker := time.NewTicker(time.Duration(timeout / 5) * time.Millisecond)
    defer ticker.Stop()

    observer := election.Observe(ctx)
    for {
        select {
        case resp, ok := <-observer:
            if !ok {
                log.Printf("Observe channel closed permaturely")
                return C.ObserverLeaderInternal
            }
            if string(resp.Kvs[0].Value) == goLeaderName {
                continue
            }
            log.Printf("Observe leaderChange, now is: %v, expect: %v",
                resp.Kvs[0].Value, goLeaderName)
            return C.ObserverLeaderChange
        case <-ticker.C:
            // 定时获取mds注册到etcd的key-value, 如果获取失败或者key
            // 不存在，返回错误，mds需要退出
            var lastErrorCode C.enum_EtcdErrCode
            for retry := 0; retry < 3; retry++ {
                lastErrorCode = C.OK
                t := time.Now()
                ctx, cancel := context.WithTimeout(context.Background(),
                        time.Duration(int(timeout) / 3) * time.Millisecond)

                resp, err := globalClient.Get(ctx, election.Key())
                errCode := GetErrCode(EtcdGet, err)

                if errCode != C.OK {
                    log.Printf("Observe can not get leader key: %v," +
                        "startTime: %v, spent: %v, etcd error: %v",
                        election.Key(), t, time.Since(t), err)
                    lastErrorCode =  C.ObserverLeaderInternal
                } else if len(resp.Kvs) == 0 {
                    log.Printf("Observe find leader key：%v not exist",
                        election.Key())
                    lastErrorCode = C.ObserverLeaderNotExist
                } else if string(resp.Kvs[0].Key) != election.Key() {
                    log.Printf("Observe leaderChange, now is: %v, expect: %v",
                        resp.Kvs[0].Value, goLeaderName)
                    lastErrorCode = C.ObserverLeaderChange
                }

                cancel()

                if lastErrorCode == C.OK {
                    break
                }
            }

            if lastErrorCode != C.OK {
                return  lastErrorCode
            }
        }
    }
}

//export EtcdLeaderResign
func EtcdLeaderResign(leaderOid uint64, timeout uint64) C.enum_EtcdErrCode {
    election := GetLeaderElection(leaderOid)
    if election == nil {
        log.Printf("can not get leader object: %v", leaderOid)
        return C.ObjectNotExist
    }

    ctx, cancel := context.WithTimeout(context.Background(),
        time.Duration(timeout) * time.Millisecond)
    defer cancel()

    var leader *clientv3.GetResponse
    var err error
    if leader, err = election.Leader(ctx);
        err != nil && err != concurrency.ErrElectionNoLeader{
        log.Printf("Leader() returned non nil err: %s", err)
        return C.LeaderResignErr
    } else if err == concurrency.ErrElectionNoLeader {
        return C.LeaderResignErr
    }

    if err := election.Resign(ctx); err != nil {
        log.Printf("%v resign leader err: %v",
            string(leader.Kvs[0].Value), err)
        return C.LeaderResignErr
    }

    log.Printf("%v resign leader success", string(leader.Kvs[0].Value))
    return C.LeaderResiginSuccess
}

//export EtcdClientGetSingleObject
func EtcdClientGetSingleObject(
    oid uint64) (C.enum_EtcdErrCode, *C.char, int) {
    if value, exist := GetManagedObject(oid); !exist {
        log.Printf("can not get object: %v", oid)
        return C.ObjectNotExist, nil, 0
    } else if res, ok := value.([]*mvccpb.KeyValue); ok {
        return C.OK, C.CString(string(res[0].Value)), len(res[0].Value)
    } else {
        log.Printf("object type err")
        return C.ErrObjectType, nil, 0
    }
}

//export EtcdClientGetMultiObject
func EtcdClientGetMultiObject(
    oid uint64, serial int) (C.enum_EtcdErrCode, *C.char, int, *C.char, int) {
    if value, exist := GetManagedObject(oid); !exist {
        return C.ObjectNotExist, nil, 0, nil, 0
    } else if res, ok := value.([]*mvccpb.KeyValue); ok {
        if serial >= len(res) {
            return C.ObjectLenNotEnough, nil, 0, nil, 0
        }
        return C.OK,
            C.CString(string(res[serial].Value)),
            len(res[serial].Value),
            C.CString(string(res[serial].Key)),
            len(res[serial].Key)
    } else {
        return C.ErrObjectType, nil, 0, nil, 0
    }
}

//export EtcdClientRemoveObject
func EtcdClientRemoveObject(oid uint64) {
    RemoveManagedObject(oid)
}

func GetLeaderElection(leaderOid uint64) *concurrency.Election {
    var election *concurrency.Election
    var ok bool
    if value, exist := GetManagedObject(leaderOid); !exist {
        log.Printf("can not get leader object: %v", leaderOid)
        return nil
    } else if election, ok = value.(*concurrency.Election); !ok {
        log.Printf("oid %v does not type of *concurrency.Election", leaderOid)
        return nil
    }

    return election
}
func main() {}
