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
	"encoding/json"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common"
	"github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology"
)

func TranslateFsType(fsType string) (common.FSType, *cmderror.CmdError) {
	fs := strings.ToUpper("TYPE_" + fsType)
	value := common.FSType_value[fs]
	var retErr cmderror.CmdError
	if value == 0 {
		retErr = *cmderror.ErrUnknownFsType()
		retErr.Format(fsType)
	}
	return common.FSType(value), &retErr
}

func TranslateBitmapLocation(bitmapLocation string) (common.BitmapLocation, *cmderror.CmdError) {
	value := common.BitmapLocation_value[bitmapLocation]
	var retErr cmderror.CmdError
	if value == 0 {
		retErr = *cmderror.ErrUnknownBitmapLocation()
		retErr.Format(bitmapLocation)
	}
	return common.BitmapLocation(value), &retErr
}

func PeerAddressToAddr(peer string) (string, *cmderror.CmdError) {
	items := strings.Split(peer, ":")
	if len(items) != 3 {
		err := cmderror.ErrSplitPeer()
		err.Format(peer)
		return "", err
	}
	return items[0] + ":" + items[1], cmderror.ErrSuccess()
}

func PeertoAddr(peer *common.Peer) (string, *cmderror.CmdError) {
	address := peer.GetAddress()
	return PeerAddressToAddr(address)
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

func topoPoolInfo2Pool(pool *topology.PoolInfo) (*Pool, *cmderror.CmdError) {
	policyJson := pool.GetRedundanceAndPlaceMentPolicy()
	var policy interface{}
	err := json.Unmarshal(policyJson, &policy)
	if err != nil {
		unmarshalErr := cmderror.ErrUnmarshalJson()
		unmarshalErr.Format(policyJson, err.Error())
		return &Pool{
			PoolID:     pool.PoolID,
			PoolName:   pool.PoolName,
			CreateTime: pool.CreateTime,
		}, unmarshalErr
	}
	return &Pool{
		PoolID:                       pool.PoolID,
		PoolName:                     pool.PoolName,
		CreateTime:                   pool.CreateTime,
		RedundanceAndPlaceMentPolicy: &policy,
	}, cmderror.ErrSuccess()
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
	Metaservers []*MetaserverInfo `json:"metaserverList"`
}

type MetaserverInfo struct {
	topology.MetaServerInfo
}

func Topology2Map(topo *topology.ListTopologyResponse) (map[string]interface{}, *cmderror.CmdError) {
	var ret = make(map[string]interface{})
	ret[CLUSTER_ID] = topo.GetClusterId()
	var errs []*cmderror.CmdError
	poolMap := make(map[uint32]*PoolInfo)
	pools := topo.GetPools().GetPoolInfos()
	for _, pool := range pools {
		poolInfo, err := topoPoolInfo2Pool(pool)
		if err.TypeCode() != cmderror.CODE_SUCCESS {
			errs = append(errs, err)
		}
		poolMap[pool.GetPoolID()] = &PoolInfo{Pool: *poolInfo}
	}

	zoneMap := make(map[uint32]*ZoneInfo)
	zones := topo.GetZones().GetZoneInfos()
	for _, zone := range zones {
		zoneMap[zone.GetZoneID()] = &ZoneInfo{ZoneInfo: *zone}

		// update pool
		pool, found := poolMap[zone.GetPoolID()]
		if !found {
			err := cmderror.ErrTopology()
			err.Format("zone", zone.GetZoneID(), "pool", zone.GetPoolID())
			errs = append(errs, err)
		}
		tmp := zoneMap[zone.GetZoneID()]
		pool.Zones = append(pool.Zones, tmp)
	}

	serverMap := make(map[uint32]*ServerInfo)
	servers := topo.GetServers().GetServerInfos()
	for _, server := range servers {
		serverMap[server.GetServerID()] = &ServerInfo{ServerInfo: *server}

		// update zone
		zone, found := zoneMap[server.GetZoneID()]
		if !found {
			err := cmderror.ErrTopology()
			err.Format("server", server.GetServerID(),
				"zone", server.GetZoneID())
			errs = append(errs, err)
		}
		tmp := serverMap[server.GetServerID()]
		zone.Servers = append(zone.Servers, tmp)
	}

	metaserverMap := make(map[uint32]*MetaserverInfo)
	metaservers := topo.GetMetaservers().GetMetaServerInfos()
	for _, metaserver := range metaservers {
		metaserverMap[metaserver.GetMetaServerID()] =
			&MetaserverInfo{MetaServerInfo: *metaserver}

		// update server
		server, found := serverMap[metaserver.GetServerId()]
		if !found {
			err := cmderror.ErrTopology()
			err.Format("metaserver", metaserver.GetMetaServerID(),
				"server", metaserver.GetServerId())
			errs = append(errs, err)
		}
		tmp := metaserverMap[metaserver.GetMetaServerID()]
		server.Metaservers = append(server.Metaservers, tmp)
	}

	poolList := make([]*PoolInfo, 0)
	for _, pool := range poolMap {
		poolList = append(poolList, pool)
	}

	ret[POOL_LIST] = poolList
	retErr := cmderror.MergeCmdErrorExceptSuccess(errs)
	return ret, retErr
}
