package helper

import (
	"fmt"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	"github.com/opencurve/curve/tools-v2/proto/proto/common"
	"strconv"
	"strings"
)

// ParsePeer parse the peer string
func ParsePeer(peer string) (*common.Peer, *cmderror.CmdError) {
	cs := strings.Split(peer, ":")
	if len(cs) != 3 {
		pErr := cmderror.ErrGetPeer()
		pErr.Format(fmt.Sprintf("ParsePeer: error format for the peer info %s", peer))
		return nil, pErr
	}
	id, err := strconv.ParseUint(cs[2], 10, 64)
	if err != nil {
		pErr := cmderror.ErrGetPeer()
		pErr.Format(fmt.Sprintf("ParsePeer: error format for the peer id %s", cs[2]))
		return nil, pErr
	}
	cs = cs[:2]
	address := strings.Join(cs, ":")
	return &common.Peer{
		Id:      &id,
		Address: &address,
	}, nil
}
