# proto
## curvebs
### proto/common.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/common.proto
### proto/scan.proto
protoc --go_out=proto --proto_path=.. \
    ../proto/scan.proto
### curvefs/proto/heartbeat.proto 
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mproto/common.proto=github.com/opencurve/curve/tools-v2/proto/proto/common \
    ../proto/heartbeat.proto

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
    ../curvefs/proto/heartbeat.proto

### curvefs/proto/mds.proto
protoc --go_out=proto --proto_path=.. \
    --go_opt=Mcurvefs/proto/common.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/common \
    --go_opt=Mcurvefs/proto/topology.proto=github.com/opencurve/curve/tools-v2/proto/curvefs/proto/topology \
    ../curvefs/proto/metaserver.proto

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
protoc --go-grpc_out=proto --proto_path=.. ../curvefs/proto/*.proto