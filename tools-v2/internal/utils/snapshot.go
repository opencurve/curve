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
 * Created Date: 2023-09-16
 * Author: baytan0720
 */

package cobrautil

import (
	"fmt"
	"net/url"
)

const (
	Version         = "0.0.6"
	TypeCloneTask   = "0"
	TypeRecoverTask = "1"

	QueryAction      = "Action"
	QueryVersion     = "Version"
	QueryUser        = "User"
	QueryUUID        = "UUID"
	QuerySource      = "Source"
	QueryDestination = "Destination"
	QueryLimit       = "Limit"
	QueryOffset      = "Offset"
	QueryStatus      = "Status"
	QueryType        = "Type"

	ActionClone               = "Clone"
	ActionRecover             = "Recover"
	ActionFlatten             = "Flatten"
	ActionCreateSnapshot      = "CreateSnapshot"
	ActionDeleteSnapshot      = "DeleteSnapshot"
	ActionCancelSnapshot      = "CancelSnapshot"
	ActionCleanCloneTask      = "CleanCloneTask"
	ActionGetCloneTaskList    = "GetCloneTaskList"
	ActionGetFileSnapshotList = "GetFileSnapshotList"
	ActionGetFileSnapshotInfo = "GetFileSnapshotInfo"

	ResultCode    = "Code"
	ResultSuccess = "0"
)

func NewSnapshotQuerySubUri(params map[string]any) string {
	values := url.Values{}

	values.Add(QueryVersion, Version)
	for key, value := range params {
		if value != "" {
			values.Add(key, fmt.Sprintf("%s", value))
		}
	}

	return "/SnapshotCloneService?" + values.Encode()
}
