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

package snapshot

import (
	"fmt"
	"strings"
)

func NewQuerySubUri(params map[string]any) string {
	values := strings.Builder{}
	for key, value := range params {
		if value != "" && value != nil {
			values.WriteString(key)
			values.WriteString("=")
			values.WriteString(fmt.Sprintf("%v", value))
			values.WriteString("&")
		}
	}

	return "/SnapshotCloneService?" + strings.TrimRight(values.String(), "&")
}

type Response struct {
	Code      string `json:"Code"`
	Message   string `json:"Message"`
	RequestId string `json:"RequestId"`
}

type SnapshotInfo struct {
	File       string  `json:"File"`
	FileLength int     `json:"FileLength"`
	Name       string  `json:"Name"`
	Progress   float64 `json:"Progress"`
	SeqNum     int     `json:"SeqNum"`
	Status     int     `json:"status"`
	Time       int64   `json:"Time"`
	UUID       string  `json:"UUID"`
	User       string  `json:"User"`
}

type SnapshotInfos []SnapshotInfo

var SnapshotStatus = []string{"done", "in-progress", "deleting", "errorDeleting", "canceling", "error"}

type TaskInfo struct {
	File       string  `json:"File"`
	FileType   int     `json:"FileType"`
	IsLazy     bool    `json:"IsLazy"`
	NextStep   int     `json:"NextStep"`
	Progress   float64 `json:"Progress"`
	Src        string  `json:"Src"`
	TaskStatus int     `json:"TaskStatus"`
	TaskType   int     `json:"TaskType"`
	Time       int64   `json:"Time"`
	UUID       string  `json:"UUID"`
	User       string  `json:"User"`
}

type TaskInfos []TaskInfo

var TaskStatus = []string{"Done", "Cloning", "Recovering", "Cleaning", "ErrorCleaning", "Error", "Retrying", "MetaInstalled"}
var TaskType = []string{"clone", "recover"}
var FileType = []string{"file", "snapshot"}
var CloneStep = []string{"CreateCloneFile", "CreateCloneMeta", "CreateCloneChunk", "CompleteCloneMeta", "RecoverChunk", "ChangeOwner", "RenameCloneFile", "CompleteCloneFile", "End"}
