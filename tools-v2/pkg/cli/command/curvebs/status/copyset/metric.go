/*
*  Copyright (c) 2023 NetEase Inc.
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
* Created Date: 2023-08-29
* Author: caoxianfei1
 */

package copyset

import (
	"fmt"
	"strings"
	"sync"

	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/spf13/cobra"
)

type QueryResult struct {
	Key    interface{}
	Err    error
	Result interface{}
	ID     uint32
}

type MetricResult QueryResult

// get raft status metric

const RAFT_STATUS_PATH = "/raft_stat"

const (
	GiB                       = 1024 * 1024 * 1024
	TIME_FORMAT               = "2006-01-02 15:04:05"
	TIME_MS_FORMAT            = "2006-01-02 15:04:05.000"
	CURVEBS_ADDRESS_DELIMITER = ","
	RAFT_REPLICAS_NUMBER      = 3
	RAFT_MARGIN               = 1000

	RAFT_EMPTY_ADDR                            = "0.0.0.0:0:0"
	RAFT_STATUS_KEY_GROUPID                    = "group_id"
	RAFT_STATUS_KEY_LEADER                     = "leader"
	RAFT_STATUS_KEY_PEERS                      = "peers"
	RAFT_STATUS_KEY_STATE                      = "state"
	RAFT_STATUS_KEY_REPLICATOR                 = "replicator"
	RAFT_STATUS_KEY_LAST_LOG_ID                = "last_log_id"
	RAFT_STATUS_KEY_SNAPSHOT                   = "snapshot"
	RAFT_STATUS_KEY_NEXT_INDEX                 = "next_index"
	RAFT_STATUS_KEY_FLYING_APPEND_ENTRIES_SIZE = "flying_append_entries_size"
	RAFT_STATUS_KEY_STORAGE                    = "storage"

	RAFT_STATUS_STATE_LEADER       = "LEADER"
	RAFT_STATUS_STATE_FOLLOWER     = "FOLLOWER"
	RAFT_STATUS_STATE_TRANSFERRING = "TRANSFERRING"
	RAFT_STATUS_STATE_CANDIDATE    = "CANDIDATE"
)

/*
[8589934645]
peer_id: 10.166.24.22:8200:0\r\n
state: LEADER\r\n
readonly: 0\r\n
term: 19\r\n
conf_index: 11244429\r\n
peers: 10.166.24.22:8200:0 10.166.24.27:8218:0 10.166.24.29:8206:0\r\n
changing_conf: NO    stage: STAGE_NONE\r\n
election_timer: timeout(1000ms) STOPPED\r\n
vote_timer: timeout(1000ms) STOPPED\r\n
stepdown_timer: timeout(1000ms) SCHEDULING(in 577ms)\r\n
snapshot_timer: timeout(1800000ms) SCHEDULING(in 277280ms)\r\n
storage: [11243647, 11245778]\n
disk_index: 11245778\n
known_applied_index: 11245778\n
last_log_id: (index=11245778,term=19)\n
state_machine: Idle\n
last_committed_index: 11245778r\n
last_snapshot_index: 11244429
last_snapshot_term: 19
snapshot_status: IDLE
replicator_82304458296217@10.166.24.27:8218:0: next_index=11245779  flying_append_entries_size=0 idle hc=17738777 ac=206514 ic=0
replicator_80702435493905@10.166.24.29:8206:0: next_index=11245779  flying_append_entries_size=0 idle hc=17738818 ac=206282 ic=0
\r\n\r\n
[8589934712]
peer_id: 10.166.24.22:8200:0
state: FOLLOWER
readonly: 0
term: 16
conf_index: 15368827
peers: 10.166.24.22:8200:0 10.166.24.29:8212:0 10.166.24.30:8219:0
leader: 10.166.24.29:8212:0
last_msg_to_now: 48
election_timer: timeout(1000ms) SCHEDULING(in 719ms)
vote_timer: timeout(1000ms) STOPPED
stepdown_timer: timeout(1000ms) STOPPED
snapshot_timer: timeout(1800000ms) SCHEDULING(in 422640ms)
storage: [15367732, 15370070]
disk_index: 15370070
known_applied_index: 15370070
last_log_id: (index=15370070,term=16)
state_machine: Idle
last_committed_index: 15370070
last_snapshot_index: 15368827
last_snapshot_term: 16
snapshot_status: IDLE
*/
func ParseRaftStatusMetric(addr string, value string) ([]map[string]string, error) {
	if value == "" || len(value) == 0 {
		return nil, fmt.Errorf("copyset raft status mertic from chunkserver is nil")
	}
	var ret []map[string]string
	items := strings.Split(value, "\r\n\r\n")
	items = items[:len(items)-1]
	for _, item := range items {
		tmap := make(map[string]string)
		lines := strings.Split(item, "\r\n")
		raplicatorIndex := 0
		for index, line := range lines {
			if index == 0 {
				start := strings.Index(line, "[") + 1
				end := strings.Index(line, "]")
				if start >= end {
					return nil, fmt.Errorf(fmt.Sprintf("format error1: %s, %s", line, addr))
				}
				tmap[RAFT_STATUS_KEY_GROUPID] = line[start:end]
				continue
			}
			hit := strings.Count(line, ": ")
			if hit == 1 {
				c := strings.Split(line, ": ")
				if len(c) != 2 {
					return nil, fmt.Errorf(fmt.Sprintf("format error2: %s, %s", line, addr))
				}
				if strings.Contains(c[0], RAFT_STATUS_KEY_REPLICATOR) {
					c[0] = fmt.Sprintf("%s%d", RAFT_STATUS_KEY_REPLICATOR, raplicatorIndex)
					raplicatorIndex += 1
				}
				tmap[c[0]] = c[1]
			} else if hit == 2 {
				// line: [changing_conf: NO    stage: STAGE_NONE]
				v := strings.Split(line, "    ")
				if len(v) != 2 {
					return nil, fmt.Errorf(fmt.Sprintf("format error3: %s, %s", line, addr))
				}
				for _, i := range v {
					j := strings.Split(i, ": ")
					if len(j) != 2 {
						return nil, fmt.Errorf(fmt.Sprintf("format error4: %s, %s", i, addr))
					}
					tmap[j[0]] = j[1]
				}
			} else if strings.Contains(line, RAFT_STATUS_KEY_STORAGE) {
				storageItems := strings.Split(line, "\n")
				for _, sitem := range storageItems {
					sitemArr := strings.Split(sitem, ": ")
					if len(sitemArr) != 2 {
						return nil, fmt.Errorf(fmt.Sprintf("format error5: %s, %s", sitem, addr))
					}
					tmap[sitemArr[0]] = sitemArr[1]
				}
			}
		}
		ret = append(ret, tmap)
	}
	return ret, nil
}

func GetRaftStatusMetric(addrs []string, results *chan MetricResult, cmd *cobra.Command) {
	timeout := config.GetFlagDuration(cmd, config.HTTPTIMEOUT)
	var wg sync.WaitGroup
	for _, host := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			metric := basecmd.NewMetric([]string{addr}, RAFT_STATUS_PATH, timeout)
			resp, err := basecmd.QueryMetric(metric)
			// resp, err := GMetricClient.GetMetricFromService(addr, RAFT_STATUS_PATH)
			*results <- MetricResult{
				Key:    addr,
				Err:    err.ToError(),
				Result: resp,
			}
		}(host)
	}
	wg.Wait()
}

// @return key: chunkserver's addr, value: copysets' raft status
func GetCopysetRaftStatus(endpoints []string, cmd *cobra.Command) (map[string][]map[string]string, error) {
	size := len(endpoints)
	results := make(chan MetricResult, size)
	GetRaftStatusMetric(endpoints, &results, cmd)
	count := 0
	ret := map[string][]map[string]string{}
	for res := range results {
		if res.Err == nil {
			v, e := ParseRaftStatusMetric(res.Key.(string), res.Result.(string))
			if e != nil {
				return nil, e
			} else {
				ret[res.Key.(string)] = v
			}
		} else {
			ret[res.Key.(string)] = nil
		}
		// quit by this way or need to close results channel
		count += 1
		if count >= size {
			break
		}
	}
	return ret, nil
}
