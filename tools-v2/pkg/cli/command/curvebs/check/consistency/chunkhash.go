package consistency

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/proto/proto/chunk"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology/statuscode"
	"google.golang.org/grpc"
)

type GetChunkHashRpc struct {
	Info        *basecmd.Rpc
	Request     *chunk.GetChunkHashRequest
	ChunkClient chunk.ChunkServiceClient
}

func (rpc *GetChunkHashRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	rpc.ChunkClient = chunk.NewChunkServiceClient(cc)
}

func (rpc *GetChunkHashRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	return rpc.ChunkClient.GetChunkHash(ctx, rpc.Request)
}

func (csCmd *ConsistencyCmd) GetChunkHash(csAddr string, lpid uint32, cid uint32, chunkId uint64) (string, *cmderror.CmdError) {
	offset := uint32(0)
	chunksize := uint32(16777216)
	request := &chunk.GetChunkHashRequest{
		LogicPoolId: &lpid,
		CopysetId:   &cid,
		ChunkId:     &chunkId,
		Offset:      &offset,
		Length:      &chunksize,
	}
	csCmd.getChunkHashRpc = &GetChunkHashRpc{
		Request: request,
	}
	var s []string
	s = append(s, csAddr)
	csCmd.getChunkHashRpc.Info = basecmd.NewRpc(s, csCmd.timeout, csCmd.retryTimes, "GetChunkHash")
	res, err := basecmd.GetRpcResponse(csCmd.getChunkHashRpc.Info, csCmd.getChunkHashRpc)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return "", err
	}
	response := res.(*chunk.GetChunkHashResponse)
	if response.GetStatus() != 0 {
		return "", cmderror.ErrBsGetChunkHash(statuscode.ChunkStatusCode(response.GetStatus()))
	}
	return response.GetHash(), cmderror.ErrSuccess()
}
func GenGroupId(poolId uint32, copysetId uint32) uint64 {
	res := (uint64(poolId) << 32) | uint64(copysetId)
	return res
}

func (csCmd *ConsistencyCmd) CheckChunkHash(csAddrs []string, lpid uint32, cid uint32, chunkid uint64) (*cmderror.CmdError, bool) {
	var preHash string
	chunkhash := true
	first := true
	gid := GenGroupId(lpid, cid)
	for _, csAddr := range csAddrs {
		curHash, err := csCmd.GetChunkHash(csAddr, lpid, cid, chunkid)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return err, chunkhash
		}
		if first {
			preHash = curHash
			first = false
			continue
		}
		if curHash != preHash {
			chunkhash = false
			var addrs []string
			var chunkserverIds []string
			base, _ := strconv.ParseInt(csCmd.port, 10, 64)
			for _, addr := range csAddrs {
				addrs = append(addrs, addr)
				res := strings.Split(addr, ":")
				port, _ := strconv.ParseInt(res[1], 10, 64)
				cksid := strconv.FormatUint(uint64(port-base), 10)
				chunkserverIds = append(chunkserverIds, cksid)
			}
			cksShow := "chunkserver" + chunkserverIds[0] + "," + "chunkserver" + chunkserverIds[1] + "," + "chunkserver" + chunkserverIds[2]
			fmt.Println("copysetId:", cid)
			fmt.Println("groupId:", gid)
			fmt.Println("logicalPoolId:", lpid)
			fmt.Println("chunkId:", chunkid)
			fmt.Println("hosts:", addrs)
			fmt.Println("chunkservers:", cksShow)
			fmt.Println()
			break
		}

	}

	if !chunkhash {
		return cmderror.ErrBsChunkHash(), chunkhash
	}
	return cmderror.ErrSuccess(), chunkhash
}

func (csCmd *ConsistencyCmd) CheckCopysetHash(cpid uint32, csAddrs []string) (*cmderror.CmdError, bool) {
	cpsHash := true
	err := &cmderror.CmdError{
		Code:    0,
		Message: "hashs are consistency",
	}
	for _, chunkid := range csCmd.chunksInCopyset[cpid] {
		lpid := csCmd.cpId2lpId[cpid]
		err1, chunkHash := csCmd.CheckChunkHash(csAddrs, lpid, cpid, chunkid)
		cpsHash = chunkHash
		if err1.TypeCode() != cmderror.CODE_SUCCESS && chunkHash {
			return err1, chunkHash
		}
		if err1.TypeCode() != cmderror.CODE_SUCCESS && !chunkHash {
			err = err1
		}
	}
	return err, cpsHash
}

// 	for csAddr, cpId := range csCmd.csAddr2Copyset {
// 		lpid := csCmd.cpId2lpId[cpId]
// 		err := csCmd.CheckChunkHash(csAddr, lpid, cpId)
// 		if csCmd.isInConsistency {
// 			for _, hErr := range csCmd.hashErr {
// 				fmt.Println(hErr.Message)
// 			}
// 		}
// 		if err.TypeCode() != cmderror.CODE_SUCCESS {
// 			return err
// 		}
// 	}
// 	return cmderror.ErrSuccess()
// }
