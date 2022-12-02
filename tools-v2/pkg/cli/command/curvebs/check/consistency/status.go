package consistency

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/proto/copyset"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"google.golang.org/grpc"
)

type GetCopysetStatusRpc struct {
	Info      *basecmd.Rpc
	Request   *copyset.CopysetStatusRequest
	CpsClient copyset.CopysetServiceClient
}

func (rpc *GetCopysetStatusRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	rpc.CpsClient = copyset.NewCopysetServiceClient(cc)
}

func (rpc *GetCopysetStatusRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return rpc.CpsClient.GetCopysetStatus(ctx, rpc.Request)
}

func (csCmd *ConsistencyCmd) GetCopysetStatus(csAddr string, lpid uint32, copysetId uint32) (*copyset.CopysetStatusResponse, *cmderror.CmdError) {
	peer := &common.Peer{
		Address: &csAddr,
	}
	hash := false
	request := &copyset.CopysetStatusRequest{
		LogicPoolId: &lpid,
		CopysetId:   &copysetId,
		Peer:        peer,
		QueryHash:   &hash,
	}
	csCmd.getCopysetStatusRpc = &GetCopysetStatusRpc{
		Request: request,
	}
	var s []string
	s = append(s, csAddr)
	csCmd.getCopysetStatusRpc.Info = basecmd.NewRpc(s, csCmd.timeout, csCmd.retryTimes, "GetCopysetStatus")
	res, err := basecmd.GetRpcResponse(csCmd.getCopysetStatusRpc.Info, csCmd.getCopysetStatusRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err
	}
	response := res.(*copyset.CopysetStatusResponse)
	if response.GetStatus() != 0 {
		return nil,
			cmderror.ErrBsGetCopysetStatus(statuscode.CopysetStatusCode(response.GetStatus()), copysetId, lpid)
	}
	return response, err
}

func (csCmd *ConsistencyCmd) CheckApplyIndex(cpid uint32, csAddrs []string) *cmderror.CmdError {
	var preIndex uint64
	var curIndex uint64
	first := true
	for _, csAddr := range csAddrs {
		lpid := csCmd.cpId2lpId[cpid]
		res, err := csCmd.GetCopysetStatus(csAddr, lpid, cpid)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		curIndex = uint64(res.GetKnownAppliedIndex())
		if first {
			preIndex = curIndex
			first = false
			continue
		}
		if curIndex != preIndex {
			err := cmderror.ErrBsApplyIndexNotEqual()
			err.Format(preIndex, curIndex, cpid)
			return err
		}
	}
	return cmderror.ErrSuccess()
}
