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
* Project: curve
* Created Date: 2023-09-21
* Author: xr-stb
 */

package clone_recover

import (
	"encoding/json"
	"fmt"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"time"
)

type CloneRecoverCmd struct {
	basecmd.FinalCurveCmd
	SnapshotAddrs []string
	Timeout       time.Duration

	User   string
	Src    string
	Dest   string
	TaskID string
	All    bool
	Failed bool
	Status string
}

func QueryTaskList(rCmd *CloneRecoverCmd) ([]map[string]string, error) {
	params := map[string]interface{}{
		cobrautil.QueryAction:      cobrautil.ActionGetCloneTaskList,
		cobrautil.QueryType:        cobrautil.TypeCloneTask,
		cobrautil.QueryUser:        rCmd.User,
		cobrautil.QueryUUID:        rCmd.TaskID,
		cobrautil.QuerySource:      rCmd.Src,
		cobrautil.QueryDestination: rCmd.Dest,
		cobrautil.QueryStatus:      rCmd.Status,
		cobrautil.QueryLimit:       100,
		cobrautil.QueryOffset:      0,
	}
	records := make([]map[string]string, 0)

	for {
		subUri := cobrautil.NewSnapshotQuerySubUri(params)
		metric := basecmd.NewMetric(rCmd.SnapshotAddrs, subUri, rCmd.Timeout)
		result, err := basecmd.QueryMetric(metric)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			return nil, err.ToError()
		}

		var resp struct {
			Code       string              `json:"Code"`
			TaskInfos  []map[string]string `json:"TaskInfos"`
			TotalCount int                 `json:"TotalCount"`
		}
		if err := json.Unmarshal([]byte(result), &resp); err != nil {
			return nil, err
		}
		if resp.Code != "0" {
			return nil, fmt.Errorf("get clone list fail, error code: %s", resp.Code)
		}
		if len(resp.TaskInfos) == 0 {
			break
		} else {
			records = append(records, resp.TaskInfos...)
			params[cobrautil.QueryOffset] = params[cobrautil.QueryOffset].(int) + params[cobrautil.QueryLimit].(int)
		}
	}

	return records, nil
}
