/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Sat 07 Aug 2021 07:57:05 PM CST
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_RAFT_NODE_H_
#define CURVEFS_SRC_METASERVER_COPYSET_RAFT_NODE_H_

#include <braft/raft.h>

#include <memory>
#include <vector>

#include "absl/memory/memory.h"
#include "curvefs/src/metaserver/copyset/types.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

// A wrapper for braft::Node
// make every memory function as virtual for GMOCK
class RaftNode {
 public:
    RaftNode(const braft::GroupId& group_id, const braft::PeerId& peer_id)
        : node_(absl::make_unique<braft::Node>(group_id, peer_id)) {}

    virtual ~RaftNode() = default;

    virtual braft::NodeId node_id() {
        return node_->node_id();
    }

    virtual braft::PeerId leader_id() {
        return node_->leader_id();
    }

    virtual bool is_leader() {
        return node_->is_leader();
    }

    virtual int init(const braft::NodeOptions& options) {
        return node_->init(options);
    }

    virtual void shutdown(braft::Closure* done) {
        node_->shutdown(done);
    }

    virtual void join() {
        node_->join();
    }

    virtual void apply(const braft::Task& task) {
        node_->apply(task);
    }

    virtual butil::Status list_peers(std::vector<braft::PeerId>* peers) {
        return node_->list_peers(peers);
    }

    virtual void add_peer(const braft::PeerId& peer, braft::Closure* done) {
        node_->add_peer(peer, done);
    }

    virtual void remove_peer(const braft::PeerId& peer, braft::Closure* done) {
        node_->remove_peer(peer, done);
    }

    virtual void change_peers(const braft::Configuration& new_peers,
                              braft::Closure* done) {
        node_->change_peers(new_peers, done);
    }

    virtual butil::Status reset_peers(const braft::Configuration& new_peers) {
        return node_->reset_peers(new_peers);
    }

    virtual void snapshot(braft::Closure* done) {
        node_->snapshot(done);
    }

    virtual void reset_election_timeout_ms(int election_timeout_ms) {
        node_->reset_election_timeout_ms(election_timeout_ms);
    }

    virtual int transfer_leadership_to(const braft::PeerId& peer) {
        return node_->transfer_leadership_to(peer);
    }

    virtual butil::Status read_committed_user_log(const int64_t index,
                                                  braft::UserLog* user_log) {
        return node_->read_committed_user_log(index, user_log);
    }

    virtual void get_status(braft::NodeStatus* status) {
        node_->get_status(status);
    }

 private:
    std::unique_ptr<braft::Node> node_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_RAFT_NODE_H_
