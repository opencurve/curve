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
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	"github.com/opencurve/curve/tools-v2/proto/proto/nameserver2"
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
)

func PeerAddressToAddr(peer string) (string, *cmderror.CmdError) {
	items := strings.Split(peer, ":")
	if len(items) != 3 {
		err := cmderror.ErrSplitPeer()
		err.Format(peer)
		return "", err
	}
	return items[0] + ":" + items[1], cmderror.ErrSuccess()
}

const (
	CLUSTER_ID = "clusterid"
	POOL_LIST  = "poollist"
)

type Pool struct {
	PoolID                       *uint32      `json:"PoolID"`
	PoolName                     *string      `json:"PoolName"`
	CreateTime                   *uint64      `json:"createTime"`
	RedundanceAndPlaceMentPolicy *interface{} `json:"redundanceAndPlaceMentPolicy"`
}

type PoolInfo struct {
	Pool
	Zones []*ZoneInfo `json:"zoneList"`
}

type ZoneInfo struct {
	topology.ZoneInfo
	Servers []*ServerInfo `json:"serverList"`
}

type ServerInfo struct {
	topology.ServerInfo
}

const (
	TYPE_DIR  = "dir"
	TYPE_FILE = "file"
)

func TranslateFileType(fileType string) (nameserver2.FileType, *cmderror.CmdError) {
	switch fileType {
	case TYPE_DIR:
		return nameserver2.FileType_INODE_DIRECTORY, cmderror.ErrSuccess()
	case TYPE_FILE:
		return nameserver2.FileType_INODE_PAGEFILE, cmderror.ErrSuccess()
	}
	retErr := cmderror.ErrBsUnknownFileType()
	retErr.Format(fileType)
	return nameserver2.FileType_INODE_DIRECTORY, retErr
}
