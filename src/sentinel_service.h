/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <string>
#include <thread>
#include <atomic>
#include <vector>
#include "nlohmann/json.hpp"

namespace pikiwidb {

namespace GroupServerRoleStrings {
  constexpr const char *Master = "master";
  constexpr const char *Slave = "slave";
}

namespace ActionState {
  constexpr const char *Nothing = "";
  constexpr const char *Synced = "synced";
  constexpr const char *SyncedFailed = "synced_failed";
}

enum class GroupState : int8_t {
  GroupServerStateNormal = 0,
  GroupServerStateSubjectiveOffline = 1,
  GroupServerStateOffline = 2
};

struct GroupInfo {
  int group_id;
  int term_id;
  std::string master_addr;
  std::string pika_sentinel_addr;
  std::vector<std::string> slaves_addr;
};

struct InfoSlave {
  std::string ip;
  std::string port;
  std::string state;
  int offset;
  int lag;
};

struct InfoReplication {
  std::string role;
  int connected_slaves;
  std::string master_host;
  std::string master_port;
  std::string master_link_status;
  uint64_t db_binlog_filenum;
  uint64_t db_binlog_offset;
  std::vector<InfoSlave> slaves;
};

struct Action {
  int index;
  std::string state;
};

struct GroupServer {
  std::string addr;
  std::string dataCenter;
  Action action;
  std::string role;
  uint64_t db_binlog_filenum;
  uint64_t db_binlog_offset;
  int8_t state;
  int8_t recall_times;
  bool replica_group;
};

struct ReplicationState  {
  int group_id;
  int index;
  std::string addr;
  GroupServer* server;
  InfoReplication replication;
  bool err;
};

struct Promoting {
  int index;
  std::string state;
};

struct Group {
  int id;
  int term_id;
  std::vector<GroupServer*> servers;
  Promoting promoting;
  bool out_of_sync;
};

class PClient;

class SentinelService {
 public:
  SentinelService();
  ~SentinelService();
  void Start();
  void Stop();
  void HTTPClient();
  void RefreshMastersAndSlavesClientWithPKPing();
  void UpdateSlaveOfflineGroups();
  void TrySwitchGroupsToNewMaster();
  void TryFixReplicationRelationships(size_t master_offline_groups);
  void CheckMastersAndSlavesState();
  void DelGroup(int index);
  void UpdateGroup(nlohmann::json jsonData);
  void CheckAndUpdateGroupServerState(GroupServer* servers, ReplicationState* state, Group* group);
  Group* GetGroup(int gid);

 private:
  void Run();
  void PKPingRedis(const std::string& addr, const nlohmann::json& jsondata, ReplicationState* state);

  std::atomic<bool> running_;
  std::thread thread_;
  std::unordered_map<int, Group*> groups_; // Save the meta information of all nodes
  std::vector<Group*> slave_offline_groups_; // Save the metadata of the offline slave node
  std::vector<Group*> master_offline_groups_; // Save the meta information about the offline primary node
  std::vector<ReplicationState*> recovered_groups_; // Save the meta information about the node that goes online again
  std::vector<ReplicationState*> states_; // Save the return information about the status value of the pkping command
  std::mutex groups_mtx_; // A lock used to protect groups
  std::string pika_sentinel_addr_;
};

}  // namespace pikiwidb