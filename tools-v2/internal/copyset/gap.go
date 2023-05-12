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
 * Created Date: 2022-07-22
 * Author: chengyi (Cyber-SiKu)
 */

package copyset

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
)

const (
	RAFT_STAT_URL       = "/raft_stat"
	KEY_PATTERN         = `\[(\d+)\]`
	STATE               = "state"
	STATE_PATTERN       = `state: ([A-Z]*)`
	LEADER              = "leader"
	LAST_LOG_ID         = "last_log_id"
	LAST_LOG_ID_PATTERN = `last_log_id: \(index=([0-9]*),term=([0-9]*)\)`
	REPLICATOR          = "replicator"
	NEXT_INDEX          = "next_index"
	NEXT_INDEX_PATTERN  = `next_index=([0-9]*)`
	FLYING              = "flying_append_entries_size"
	FLYING_PATTERN      = `flying_append_entries_size=([0-9]*)`
	SNAPSHOT            = "installing snapshot"
)

type CopysetLeaderInfo struct {
	Gap      uint64
	Snapshot bool // installing snapshot
}

func NewCopysetLeaderInfo() *CopysetLeaderInfo {
	return &CopysetLeaderInfo{
		Gap:      0,
		Snapshot: false,
	}
}

// get the gap of the leader copyset located in addr
// return copysetKety to gap
// looks like:
// [4294967297]
// peer_id: 10.219.192.50:6801:0
// state: FOLLOWER
// readonly: 0
// term: 4
// conf_index: 4
// peers: 10.219.192.50:6800:0 10.219.192.50:6801:0 10.219.192.50:6802:0
// leader: 10.219.192.50:6800:0
// last_msg_to_now: 81
// election_timer: timeout(1000ms) SCHEDULING(in 1018ms)
// vote_timer: timeout(1000ms) STOPPED
// stepdown_timer: timeout(1000ms) STOPPED
// snapshot_timer: timeout(300000ms) SCHEDULING(in 40598ms)
// storage: [5, 4]
// disk_index: 4
// known_applied_index: 4
// last_log_id: (index=4,term=4)
// state_machine: Idle
// last_committed_index: 4
// last_snapshot_index: 4
// last_snapshot_term: 4
// snapshot_status: IDLE
//
// [4294967302]
// peer_id: 10.219.192.50:6801:0
// state: LEADER
// readonly: 0
// term: 3
// conf_index: 3
// peers: 10.219.192.50:6800:0 10.219.192.50:6801:0 10.219.192.50:6802:0
// changing_conf: NO    stage: STAGE_NONE
// election_timer: timeout(1000ms) STOPPED
// vote_timer: timeout(1000ms) STOPPED
// stepdown_timer: timeout(1000ms) SCHEDULING(in 0ms)
// snapshot_timer: timeout(300000ms) SCHEDULING(in 36623ms)
// storage: [4, 3]
// disk_index: 3
// known_applied_index: 3
// last_log_id: (index=3,term=3)
// state_machine: Idle
// last_committed_index: 3
// last_snapshot_index: 3
// last_snapshot_term: 3
// snapshot_status: IDLE
// replicator_3298534883333@10.219.192.50:6800:0: next_index=4  flying_append_entries_size=0 idle hc=150785 ac=2 ic=0
// replicator_2203318222854@10.219.192.50:6802:0: next_index=4  flying_append_entries_size=0 idle hc=150785 ac=2 ic=0
func GetLeaderCopysetGap(addr string, key2LeaderInfo *sync.Map, timeout time.Duration) *cmderror.CmdError {
	metric := basecmd.NewMetric([]string{addr}, RAFT_STAT_URL, timeout)
	result, _ := basecmd.QueryMetric(metric)
	lines := strings.Split(result, "\n")
	length := len(lines)
	index := 0
	for index < length && len(lines[index]) > 0 && lines[index][0] == '[' {
		keyRegexp := regexp.MustCompile(KEY_PATTERN)
		params := keyRegexp.FindStringSubmatch(lines[index])
		if len(params) != 2 {
			parseErr := cmderror.ErrCopysetGapKey()
			parseErr.Format(lines[index])
			return parseErr
		}
		key, _ := strconv.ParseUint(params[1], 10, 64)

		nextIndex := index + 1
		leaderFlag := false
		stateFlag := false
		lastLogIdFlag := false
		replicatorFlag := false
		lastLogId := uint64(0)
		nextLogId := uint64(0)
		flyingId := uint64(0)
		gap := uint64(0)
		snapshot := false
		for nextIndex < length && len(lines[nextIndex]) > 0 && lines[nextIndex][0] != '[' {
			line := lines[nextIndex]
			if !leaderFlag && !stateFlag && strings.HasPrefix(line, STATE) {
				// state: LEADER
				stateRegexp := regexp.MustCompile(STATE_PATTERN)
				params := stateRegexp.FindStringSubmatch(line)
				if len(params) == 2 && strings.ToLower(params[1]) == LEADER {
					leaderFlag = true
				} else if len(params) != 2 {
					parseErr := cmderror.ErrCopysetGapState()
					parseErr.Format(key, line)
					return parseErr
				}
				stateFlag = true
			} else if leaderFlag && !lastLogIdFlag && strings.HasPrefix(line, LAST_LOG_ID) {
				// last_log_id: (index=3,term=3)
				// lastLogId
				stateRegexp := regexp.MustCompile(LAST_LOG_ID_PATTERN)
				params := stateRegexp.FindStringSubmatch(line)
				if len(params) == 3 {
					lastLogId, _ = strconv.ParseUint(params[1], 10, 64)
				} else {
					parseErr := cmderror.ErrCopysetGapLastLogId()
					parseErr.Format(key, line)
					return parseErr
				}
				lastLogIdFlag = true
			} else if leaderFlag && strings.HasPrefix(line, REPLICATOR) {
				// replicator_2203318222854@10.219.192.50:6802:0: next_index=4  flying_append_entries_size=0 idle hc=150785 ac=2 ic=0
				if strings.Contains(line, SNAPSHOT) {
					snapshot = true
				}
				nextIndexRegexp := regexp.MustCompile(NEXT_INDEX_PATTERN)
				nextIndexParams := nextIndexRegexp.FindStringSubmatch(line)
				if len(nextIndexParams) == 2 {
					nextLogId, _ = strconv.ParseUint(nextIndexParams[1], 10, 64)
				} else {
					pareseErr := cmderror.ErrCopysetGapReplicator()
					pareseErr.Format(key, line)
					return pareseErr
				}

				flyingIdRegexp := regexp.MustCompile(FLYING_PATTERN)
				flyingIdParams := flyingIdRegexp.FindStringSubmatch(line)
				if len(flyingIdParams) == 2 {
					flyingId, _ = strconv.ParseUint(flyingIdParams[1], 10, 64)
				} else {
					pareseErr := cmderror.ErrCopysetGapReplicator()
					pareseErr.Format(key, line)
					return pareseErr
				}
				replicatorFlag = true
				if lastLogIdFlag && gap < lastLogId-(nextLogId-flyingId) {
					gap = lastLogId - (nextLogId - flyingId - 1)
					if leaderFlag && !(lastLogIdFlag && replicatorFlag) {
						// leader, but no last_log_id or replicator
						pareseErr := cmderror.ErrCopysetGap()
						pareseErr.Format(key)
						return pareseErr
					} else {
						leader := NewCopysetLeaderInfo()
						leader.Gap = gap
						leader.Snapshot = snapshot
						act, loaded := key2LeaderInfo.LoadOrStore(key, leader)
						if loaded {
							actLeader := act.(*CopysetLeaderInfo)
							if actLeader.Gap < gap ||
								(!actLeader.Snapshot && snapshot) {
								// act need update
								if actLeader.Gap > gap {
									leader.Gap = actLeader.Gap
								} else if actLeader.Snapshot && !snapshot {
									leader.Snapshot = actLeader.Snapshot
								}
								key2LeaderInfo.Store(key, leader)
							}
						}
					}
				}
			}
			nextIndex++
		}

		index = nextIndex
	}
	return cmderror.ErrSuccess()
}
