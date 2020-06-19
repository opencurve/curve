/*
 * Project: curve
 * Created Date: 2020-06-19
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_RAFT_NODE_H_
#define SRC_CHUNKSERVER_RAFT_NODE_H_

#include <braft/raft.h>
#include <memory>
#include <vector>
#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

class RaftNode {
 public:
    RaftNode(const GroupId& group_id, const PeerId& peer_id) :
        node_(std::make_shared<Node>(group_id, peer_id)) {}
    virtual ~RaftNode() = default;

    virtual braft::NodeId node_id() {
        return node_->node_id();
    }

    virtual PeerId leader_id() {
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

    virtual void get_status(NodeStatus* status) {
        node_->get_status(status);
    }

 private:
    std::shared_ptr<Node> node_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFT_NODE_H_
