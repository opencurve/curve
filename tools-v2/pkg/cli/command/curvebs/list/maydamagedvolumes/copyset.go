package maydamagedvolumes

import (
	"context"
	"strconv"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"google.golang.org/grpc"
)

type GetCopySetsInChunkServerRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.GetCopySetsInChunkServerRequest
	TopologyClient topology.TopologyServiceClient
}

func (gcpsRpc *GetCopySetsInChunkServerRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	gcpsRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (gcpsRpc *GetCopySetsInChunkServerRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return gcpsRpc.TopologyClient.GetCopySetsInChunkServer(ctx, gcpsRpc.Request)
}

type ListVolumesOnCopysetsRpc struct {
	Info    *basecmd.Rpc
	Request *nameserver2.ListVolumesOnCopysetsRequest
	Client  nameserver2.CurveFSServiceClient
}

func (lvRpc *ListVolumesOnCopysetsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lvRpc.Client = nameserver2.NewCurveFSServiceClient(cc)
}

func (lvRpc *ListVolumesOnCopysetsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lvRpc.Client.ListVolumesOnCopysets(ctx, lvRpc.Request)
}

func (mdvCmd *MayDamVolCmd) GetCopySetsOnOffLineCs() *cmderror.CmdError {
	for _, csAddr := range mdvCmd.offLineCsAddrs {
		split := strings.Split(csAddr, ":")
		ip := split[0]
		port_string := split[1]
		p, _ := strconv.ParseUint(port_string, 10, 64)
		port := uint32(p)
		request := &topology.GetCopySetsInChunkServerRequest{
			HostIp: &ip,
			Port:   &port,
		}
		mdvCmd.getCopySetsInChunkServerRpc = &GetCopySetsInChunkServerRpc{
			Request: request,
		}

		mdvCmd.getCopySetsInChunkServerRpc.Info = basecmd.NewRpc(mdvCmd.addrs, mdvCmd.timeout, mdvCmd.retryTimes, "GetCopySetsInChunkServer")
		result, err := basecmd.GetRpcResponse(mdvCmd.getCopySetsInChunkServerRpc.Info, mdvCmd.getCopySetsInChunkServerRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err
		}
		response := result.(*topology.GetCopySetsInChunkServerResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrGetCopySetsInChunkServer(statuscode.TopoStatusCode(response.GetStatusCode()), csAddr)
		}
		mdvCmd.copysetInfos = append(mdvCmd.copysetInfos, response.GetCopysetInfos()...)
	}

	return cmderror.ErrSuccess()
}

func (mdvCmd *MayDamVolCmd) CountOffLineCopysets() bool {
	hasDamagedCopyset := false
	for _, cpsInfo := range mdvCmd.copysetInfos {
		cpyId := cpsInfo.GetCopysetId()
		mdvCmd.copysetsCnt[cpyId]++
		if mdvCmd.copysetsCnt[cpyId] == 2 {
			mdvCmd.damagedCps = append(mdvCmd.damagedCps, cpsInfo)
			hasDamagedCopyset = true
		}
	}
	return hasDamagedCopyset
}

func (mdvCmd *MayDamVolCmd) ListVolsOnDamagedCps() *cmderror.CmdError {
	request := &nameserver2.ListVolumesOnCopysetsRequest{
		Copysets: mdvCmd.damagedCps,
	}
	mdvCmd.listVolsOnCpysRpc = &ListVolumesOnCopysetsRpc{
		Request: request,
	}
	mdvCmd.listVolsOnCpysRpc.Info = basecmd.NewRpc(mdvCmd.addrs, mdvCmd.timeout, mdvCmd.retryTimes, "ListVolumesOnCopysets")
	result, err := basecmd.GetRpcResponse(mdvCmd.listVolsOnCpysRpc.Info, mdvCmd.listVolsOnCpysRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}
	response := result.(*nameserver2.ListVolumesOnCopysetsResponse)
	if response.GetStatusCode() != 0 {
		return cmderror.ErrListVolsOnDamagedCps(nameserver2.StatusCode(response.GetStatusCode()))
	}
	mdvCmd.fileNames = append(mdvCmd.fileNames, response.GetFileNames()...)
	return cmderror.ErrSuccess()
}
