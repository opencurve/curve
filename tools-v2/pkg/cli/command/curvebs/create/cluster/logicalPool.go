package cluster

import (
	"github.com/opencurve/curve/tools-v2/proto/proto/topology"
)

type LogicalPool struct {
	CopysetNum   uint32                   `json:"copysetnum"`
	Name         string                   `json:"name"`
	PhysicalPool string                   `json:"physicalpool"`
	ReplicasNum  uint32                   `json:"replicasnum"`
	ScatterWidth uint32                   `json:"scatterwidth"`
	Type         topology.LogicalPoolType `json:"type"`
	ZoneNum      uint32                   `json:"zonenum"`
}
