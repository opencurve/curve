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
* Created Date: 2023-08-23
* Author: lng2020
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
	version = "0.0.6"
)

type CloneOrRecover struct {
	serverAddress []string
	timeout       time.Duration

	User string
	Src  string
	Dest string
	Lazy bool
}

func newCloneOrRecover(serverAddress []string, timeout time.Duration, user, src, dest string, lazy bool) *CloneOrRecover {
	return &CloneOrRecover{
		serverAddress: serverAddress,
		timeout:       timeout,
		User:          user,
		Src:           src,
		Dest:          dest,
		Lazy:          lazy,
	}
}

type QueryParams struct {
	Action      string `json:"Action"`
	Version     string `json:"Version"`
	User        string `json:"User"`
	Source      string `json:"Source"`
	Destination string `json:"Destination"`
	Lazy        string `json:"Lazy"`
}

func (c *CloneOrRecover) CreateRecover() error {
	params := QueryParams{
		Action:      "Recover",
		Version:     version,
		User:        c.User,
		Source:      c.Src,
		Destination: c.Dest,
		Lazy:        strconv.FormatBool(c.Lazy),
	}

	var resp struct {
		Code string
	}
	err := c.query(params, &resp)
	if err != nil || resp.Code != "0" {
		return fmt.Errorf("create recover failed, error=%s", err)
	}

	return nil
}

func (c *CloneOrRecover) CreateClone() error {
	params := QueryParams{
		Action:      "Clone",
		Version:     version,
		User:        c.User,
		Source:      c.Src,
		Destination: c.Dest,
		Lazy:        strconv.FormatBool(c.Lazy),
	}

	var resp struct {
		Code string
	}
	err := c.query(params, &resp)
	if err != nil || resp.Code != "0" {
		return fmt.Errorf("create clone failed, error=%s", err)
	}

	return nil
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
		"Source":      params.Source,
		"Destination": params.Destination,
		"Lazy":        params.Lazy,
	}

	for key, value := range paramsMap {
		if value != "" {
			values.Add(key, value)
		}
	}

	return values.Encode()
}
