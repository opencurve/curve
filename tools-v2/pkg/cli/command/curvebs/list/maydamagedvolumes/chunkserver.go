package maydamagedvolumes

import (
	"context"
	"fmt"
	"strconv"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/chunk"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"google.golang.org/grpc"
)

type ListChunkServersRpc struct {
	Info           *basecmd.Rpc
	Request        *topology.ListChunkServerRequest
	TopologyClient topology.TopologyServiceClient
}

func (lcsRpc *ListChunkServersRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	lcsRpc.TopologyClient = topology.NewTopologyServiceClient(cc)
}

func (lcsRpc *ListChunkServersRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return lcsRpc.TopologyClient.ListChunkServer(ctx, lcsRpc.Request)
}

type GetChunkInfoRpc struct {
	Info        *basecmd.Rpc
	Request     *chunk.GetChunkInfoRequest
	ChunkClient chunk.ChunkServiceClient
}

func (ciRpc *GetChunkInfoRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	ciRpc.ChunkClient = chunk.NewChunkServiceClient(cc)
}

func (ciRpc *GetChunkInfoRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return ciRpc.ChunkClient.GetChunkInfo(ctx, ciRpc.Request)
}

func (mdvCmd *MayDamVolCmd) ListChunkServersOnServers() *cmderror.CmdError {
	// fmt.Println("ListChunkServersOnServers() in")
	for _, server := range mdvCmd.clusterServersInfo {
		sid := server.GetServerID()
		request := &topology.ListChunkServerRequest{
			ServerID: &sid,
		}
		mdvCmd.listChunkServersRpc = &ListChunkServersRpc{
			Request: request,
		}
		mdvCmd.listChunkServersRpc.Info = basecmd.NewRpc(mdvCmd.addrs, mdvCmd.timeout, mdvCmd.retryTimes, "ListChunkServer")
		result, err := basecmd.GetRpcResponse(mdvCmd.listChunkServersRpc.Info, mdvCmd.listChunkServersRpc)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			fmt.Println("listChunkServersRpc fail")
			return err
		}
		response := result.(*topology.ListChunkServerResponse)
		if response.GetStatusCode() != 0 {
			return cmderror.ErrListChunkServers(statuscode.TopoStatusCode(response.GetStatusCode()))
		}

		for _, chunkServer := range response.GetChunkServerInfos() {
			if chunkServer.GetStatus() == *topology.ChunkServerStatus_RETIRED.Enum() {
				mdvCmd.isRetired = true
				continue
			}
			//fmt.Println(chunkServer)
			mdvCmd.chunkServerInfos = append(mdvCmd.chunkServerInfos, chunkServer)
		}
	}
	return cmderror.ErrSuccess()
}

func (mdvCmd *MayDamVolCmd) CheckIfChunkServerOnline(csAddr string) bool {
	// fmt.Printf("CheckIfChunkServerOnline() in, csAddr is csAddr %s", csAddr)
	lpid := uint32(1)
	cid := uint32(1)
	chunkid := uint64(1)
	request := &chunk.GetChunkInfoRequest{
		LogicPoolId: &lpid,
		CopysetId:   &cid,
		ChunkId:     &chunkid,
	}
	mdvCmd.getChunkInfoRpc = &GetChunkInfoRpc{
		Request: request,
	}
	var s []string
	s = append(s, csAddr)
	mdvCmd.getChunkInfoRpc.Info = basecmd.NewRpc(s, mdvCmd.timeout, mdvCmd.retryTimes, "GetChunkInfo")
	_, err := basecmd.GetRpcResponse(mdvCmd.getChunkInfoRpc.Info, mdvCmd.getChunkInfoRpc)
	return err.TypeCode() == cmderror.CODE_SUCCESS
}

func (mdvCmd *MayDamVolCmd) FindOffLineChunkServers() *cmderror.CmdError {
	// fmt.Println("FindOffLineChunkServers() in")
	for _, cs := range mdvCmd.chunkServerInfos {
		csAddr := cs.GetHostIp() + ":" + strconv.FormatUint(uint64(cs.GetPort()), 10)
		if !mdvCmd.CheckIfChunkServerOnline(csAddr) {
			mdvCmd.offLineCsAddrs = append(mdvCmd.offLineCsAddrs, csAddr)
		}
	}
	return cmderror.ErrSuccess()
}
