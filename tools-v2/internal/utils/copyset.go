/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: CurveCli
 * Created Date: 2022-06-21
 * Author: chengyi (Cyber-SiKu)
 */

package cobrautil

import (
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	fscopyset "github.com/opencurve/curve/tools-v2/proto/curvefs/proto/copyset"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/heartbeat"
	bscopyset "github.com/opencurve/curve/tools-v2/proto/proto/copyset"
	bsheartbeat "github.com/opencurve/curve/tools-v2/proto/proto/heartbeat"
)

type FsCopysetInfoStatus struct {
	Info        *heartbeat.CopySetInfo                      `json:"info,omitempty"`
	Peer2Status map[string]*fscopyset.CopysetStatusResponse `json:"peer status,omitempty"`
}

type BsCopysetInfoStatus struct {
	Info        *bsheartbeat.CopySetInfo                    `json:"info,omitempty"`
	Peer2Status map[string]*bscopyset.CopysetStatusResponse `json:"peer status,omitempty"`
}

type COPYSET_STATE uint32

const (
	STATE_LEADER        COPYSET_STATE = 1
	STATE_TRANSFERRING  COPYSET_STATE = 2
	STATE_CANDIDATE     COPYSET_STATE = 3
	STATE_FOLLOWER      COPYSET_STATE = 4
	STATE_ERROR         COPYSET_STATE = 5
	STATE_UNINITIALIZED COPYSET_STATE = 6
	STATE_SHUTTING      COPYSET_STATE = 7
	STATE_SHUTDOWN      COPYSET_STATE = 8
)

var (
	CopysetState_name = map[uint32]string{
		1: "leader",
		2: "transferring",
		3: "candidate",
		4: "follower",
		5: "error",
		6: "uninitialized",
		7: "shutting",
		8: "shutdown",
	}
	CopysetState_Avaliable = map[uint32]bool{
		1: true,
		2: true,
		3: true,
		4: true,
		5: false,
		6: false,
		7: false,
		8: false,
	}
)

type COPYSET_CHECK_RESULT int32

const (
	HEALTHY COPYSET_CHECK_RESULT = iota
	PARSE_ERROR
	PEERS_NO_SUFFICIENT
	LOG_INDEX_GAP_TOO_BIG
	INSTALLING_SNAPSHOT
	MINORITY_NOT_ONLINE
	MAJORITY_NOT_ONLINE
	OTHER_ERROR
)

var (
	CopysetResultStr = map[COPYSET_CHECK_RESULT]string{
		0: "healthy",
		1: "parse error",
		2: "peers no sufficient",
		3: "log index gap too big",
		4: "some peers are installing snapshot",
		5: "minority peers are not online",
		6: "majority peers are not online",
		7: "other error",
	}
)

// The copyset is stored in n peers.
// Note the number of available peers as p.
// When p==n, the copy status is ok;
// when n>p>=n/2+1, the copy status is warn;
// when p<=n/2, the copy status is error.
//
// The available status of the peer is online,
// and the op_status obtained from the copyStatus on the peer is ok,
// and the state of the copyStatus on the peer is the available status.
//
// For the state available status of copysetStatus,
// please refer to CopysetState_Avaliable.
func CheckFsCopySetHealth(copysetIS *FsCopysetInfoStatus) (COPYSET_HEALTH_STATUS, []*cmderror.CmdError) {
	peers := copysetIS.Info.GetPeers()
	peer2Status := copysetIS.Peer2Status
	avalibalePeerNum := 0
	var errs []*cmderror.CmdError
	for addr, status := range peer2Status {
		if status == nil {
			// peer is offline
			err := cmderror.ErrOfflineCopysetPeer()
			err.Format(addr)
			errs = append(errs, err)
			continue
		}
		opStatus := status.GetStatus()
		state := status.GetCopysetStatus().GetState()
		peer := status.GetCopysetStatus().GetPeer()
		if opStatus == fscopyset.COPYSET_OP_STATUS_COPYSET_OP_STATUS_SUCCESS && CopysetState_Avaliable[state] {
			avalibalePeerNum++
		} else if opStatus != fscopyset.COPYSET_OP_STATUS_COPYSET_OP_STATUS_SUCCESS {
			err := cmderror.ErrCopysetOpStatus(opStatus, addr)
			errs = append(errs, err)
		} else {
			err := cmderror.ErrStateCopysetPeer()
			err.Format(peer.String(), CopysetState_name[state])
			errs = append(errs, err)
		}
	}

	n := len(peers)
	switch {
	case avalibalePeerNum == n:
		return COPYSET_OK, errs
	case avalibalePeerNum >= n/2+1:
		return COPYSET_WARN, errs
	default:
		return COPYSET_ERROR, errs
	}
}

func GetCopysetKey(poolid uint64, copysetid uint64) uint64 {
	return (poolid << 32) | copysetid
}

// return poolid, copysetid
func CopysetKey2PoolidCopysetid(copysetKey uint64) (uint32, uint32) {
	poolid := copysetKey >> 32
	copysetid := copysetKey & (1<<32 - 1)
	return uint32(poolid), uint32(copysetid)
}
