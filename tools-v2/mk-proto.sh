# proto dir
mkdir -p proto
# proto
protoc --go_out=proto --proto_path=internal/proto \
    internal/proto/curvebs/topology/statuscode.proto
protoc --go_out=proto --proto_path=internal/proto \
    internal/proto/curvebs/schedule/statuscode.proto
## curvebs
### proto/chunk.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/chunk.proto
### proto/chunkserver.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/chunkserver.proto
### proto/cli.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/cli.proto
### proto/cli2.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mproto/common.proto=github.com/opencurve/curve/tools-v2/proto/proto/common \
    ../proto/cli2.proto
### proto/common.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/common.proto
### proto/configuration.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/configuration.proto
### proto/copyset.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mproto/common.proto=github.com/opencurve/curve/tools-v2/proto/proto/common \
    ../proto/copyset.proto
### proto/curve_storage.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/curve_storage.proto
### proto/scan.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/scan.proto
### proto/heartbeat.proto 
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mproto/common.proto=github.com/opencurve/curve/tools-v2/proto/proto/common \
    --go_opt=Mproto/scan.proto=github.com/opencurve/curve/tools-v2/proto/proto/scan \
    ../proto/heartbeat.proto
### proto/topology
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mproto/common.proto=github.com/opencurve/curve/tools-v2/proto/proto/common \
    ../proto/topology.proto
### proto/integrity.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/integrity.proto
### proto/nameserver2.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mproto/common.proto=github.com/opencurve/curve/tools-v2/proto/proto/common \
    ../proto/nameserver2.proto
### proto/schedule.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/schedule.proto
### proto/snapshotcloneserver.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/snapshotcloneserver.proto

## curvefs
### curvefs/proto/cli2.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mcurvefs/proto/common.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common \
    ../curvefs/proto/cli2.proto

### curvefs/proto/common.proto
protoc --go_out=proto --proto_path=.. \
    ../curvefs/proto/common.proto

### curvefs/proto/copyset.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mcurvefs/proto/common.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common \
    ../curvefs/proto/copyset.proto

### curvefs/proto/heartbeat.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mcurvefs/proto/common.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common \
    --go_opt=Mproto/heartbeat.proto=github.com/opencurve/curve/tools-v2/proto/proto/heartbeat \
    --go_opt=Mcurvefs/proto/metaserver.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/metaserver \
    ../curvefs/proto/heartbeat.proto

### curvefs/proto/mds.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mcurvefs/proto/common.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common \
    --go_opt=Mcurvefs/proto/topology.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology \
    ../curvefs/proto/mds.proto

### curvefs/proto/metaserver.proto 
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mcurvefs/proto/common.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common \
    ../curvefs/proto/metaserver.proto

### curvefs/proto/schedule.proto
protoc --go_out=proto --proto_path=.. \
    ../curvefs/proto/schedule.proto

### curvefs/proto/space.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mcurvefs/proto/common.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common \
    ../curvefs/proto/space.proto

### curvefs/proto/topology.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mcurvefs/proto/common.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common \
    --go_opt=Mcurvefs/proto/heartbeat.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/heartbeat \
    ../curvefs/proto/topology.proto

# grpc
## bs
protoc --go-grpc_out=proto --proto_path=.. ../proto/*.proto
## fs
protoc --go-grpc_out=proto --proto_path=.. ../curvefs/proto/*.proto