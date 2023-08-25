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
* Created Date: 2023-08-21
* Author: setcy
 */

package volume

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
)

const (
	cloneTaskType   = "0"
	recoverTaskType = "1"
	version         = "0.0.6"
)

type CloneOrRecover struct {
	serverAddress []string
	timeout       time.Duration

	User   string
	Src    string
	Dest   string
	TaskID string
	All    bool
	Failed bool

	messages []string
}

func newCloneOrRecover(serverAddress []string, timeout time.Duration, user, src, dest, taskID string, all, failed bool) *CloneOrRecover {
	return &CloneOrRecover{
		serverAddress: serverAddress,
		timeout:       timeout,
		User:          user,
		Src:           src,
		Dest:          dest,
		TaskID:        taskID,
		All:           all,
		Failed:        failed,
	}
}

type Record struct {
	UUID string `json:"UUID"`
	User string `json:"User"`
	Src  string `json:"Src"`
	File string `json:"File"`

	Result string `json:"Result"`
}

func (c *CloneOrRecover) queryCloneOrRecoverBy(taskType string) []*Record {
	status := ""
	if c.Failed {
		status = "5"
	}

	records := c.getCloneListAll(taskType, status)

	return records
}

func (c *CloneOrRecover) getCloneListAll(cloneType, status string) []*Record {
	limit := 100
	offset := 0
	var receiveRecords []*Record

	for {
		records := c.getCloneList(cloneType, status, limit, offset)
		if len(records) == 0 {
			break
		}
		receiveRecords = append(receiveRecords, records...)
		offset += limit
	}

	return receiveRecords
}

type QueryParams struct {
	Action      string `json:"Action"`
	Version     string `json:"Version"`
	User        string `json:"User"`
	UUID        string `json:"UUID"`
	Source      string `json:"Source"`
	Destination string `json:"Destination"`
	Limit       string `json:"Limit"`
	Offset      string `json:"Offset"`
	Status      string `json:"Status"`
	Type        string `json:"Type"`
}

func (c *CloneOrRecover) getCloneList(cloneType, status string, limit, offset int) []*Record {
	params := QueryParams{
		Action:      "GetCloneTaskList",
		Version:     version,
		User:        c.User,
		UUID:        c.TaskID,
		Source:      c.Src,
		Destination: c.Dest,
		Limit:       strconv.Itoa(limit),
		Offset:      strconv.Itoa(offset),
		Status:      status,
		Type:        cloneType,
	}

	var resp struct {
		Code       string
		Message    string
		RequestId  string
		TaskInfos  []*Record
		TotalCount int
	}
	err := c.query(params, &resp)
	if err != nil || resp.Code != "0" {
		fmt.Printf("get clone list fail, error=%s\n", err)
		return nil
	}

	return resp.TaskInfos
}

func (c *CloneOrRecover) query(params QueryParams, data interface{}) error {
	encodedParams := c.encodeParam(params)

	subUri := fmt.Sprintf("/SnapshotCloneService?%s", encodedParams)

	metric := basecmd.NewMetric(c.serverAddress, subUri, c.timeout)

	result, err := basecmd.QueryMetric(metric)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err.ToError()
	}

	if err := json.Unmarshal([]byte(result), &data); err != nil {
		return err
	}

	return nil
}

func (c *CloneOrRecover) encodeParam(params QueryParams) string {
	values := url.Values{}
	paramsMap := map[string]string{
		"Action":      params.Action,
		"Version":     params.Version,
		"User":        params.User,
		"UUID":        params.UUID,
		"Source":      params.Source,
		"Destination": params.Destination,
		"Limit":       params.Limit,
		"Offset":      params.Offset,
		"Status":      params.Status,
		"Type":        params.Type,
	}

	for key, value := range paramsMap {
		if value != "" {
			values.Add(key, value)
		}
	}

	return values.Encode()
}

func (c *CloneOrRecover) cleanCloneOrRecover(taskID, user string) error {
	params := QueryParams{
		Action:  "CleanCloneTask",
		Version: version,
		User:    user,
		UUID:    taskID,
	}

	err := c.query(params, nil)
	if err != nil {
		return err
	}

	return err
}
