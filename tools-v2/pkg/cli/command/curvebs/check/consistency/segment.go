package consistency

import (
	"context"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

type GetOrAllcSegInfoRpc struct {
	Info      *basecmd.Rpc
	Request   *nameserver2.GetOrAllocateSegmentRequest
	mdsClient nameserver2.CurveFSServiceClient
}

func (rpc *GetOrAllcSegInfoRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	rpc.mdsClient = nameserver2.NewCurveFSServiceClient(cc)
}

func (rpc *GetOrAllcSegInfoRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return rpc.mdsClient.GetOrAllocateSegment(ctx, rpc.Request)
}

func (csCmd *ConsistencyCmd) GetFileSegInfo(fileName string, offset uint64) (*nameserver2.GetOrAllocateSegmentResponse, *cmderror.CmdError) {
	owner := config.GetBsFlagString(csCmd.Cmd, config.CURVEBS_USER)
	password := config.GetBsFlagString(csCmd.Cmd, config.CURVEBS_PASSWORD)
	date, errDat := cobrautil.GetTimeofDayUs()
	allcate := false
	if errDat.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, errDat
	}
	request := &nameserver2.GetOrAllocateSegmentRequest{
		FileName:           &fileName,
		Offset:             &offset,
		Owner:              &owner,
		Date:               &date,
		AllocateIfNotExist: &allcate,
	}
	if owner == viper.GetString(config.VIPER_CURVEBS_USER) && len(password) != 0 {
		strSig := cobrautil.GetString2Signature(date, owner)
		sig := cobrautil.CalcString2Signature(strSig, password)
		request.Signature = &sig
	}
	csCmd.getOrAllcSegInfoRpc = &GetOrAllcSegInfoRpc{
		Request: request,
	}
	csCmd.getOrAllcSegInfoRpc.Info = basecmd.NewRpc(csCmd.addrs, csCmd.timeout, csCmd.retryTimes, "GetOrAllcSegInfo")
	res, err := basecmd.GetRpcResponse(csCmd.getOrAllcSegInfoRpc.Info, csCmd.getOrAllcSegInfoRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return nil, err
	}
	response := res.(*nameserver2.GetOrAllocateSegmentResponse)
	if response.GetStatusCode() != 0 {
		return nil, cmderror.ErrBsGetSegInfo(statuscode.GetSegmentRes(response.GetStatusCode()))
	}
	return response, cmderror.ErrSuccess()
}
func (csCmd *ConsistencyCmd) GetFileSegments(fileName string, fileInfo *nameserver2.FileInfo) ([]*nameserver2.PageFileSegment, *cmderror.CmdError) {
	segSize := fileInfo.GetSegmentSize()
	segNum := fileInfo.GetLength() / uint64(segSize)
	var pfSegs []*nameserver2.PageFileSegment
	var i uint64
	for i = 0; i < segNum; i++ {
		res, err := csCmd.GetFileSegInfo(fileName, i*uint64(segSize))
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			continue
		}
		pfs := res.PageFileSegment
		pfSegs = append(pfSegs, pfs)
	}
	return pfSegs, cmderror.ErrSuccess()
}
