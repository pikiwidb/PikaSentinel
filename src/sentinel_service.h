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
#include <set>
#include <vector>

namespace pikiwidb {

class PClient;

class SentinelService {
 public:
  SentinelService();
  ~SentinelService();
  void Start();
  void Stop();
  void SwitchMaster(const int group_id);
  void LoadMeta(const std::string& message);
  void RequestData();
  std::string DeCodeIp(const std::string& serveraddr);
  int DeCodePort(const std::string& serveraddr);

 private:
  void Run();
  bool PKPingRedis(const std::string& host, const int port, const int group_id);
  bool PingRedis(const std::string& host, const int port);
  bool Slaveof(const std::string& host, const int port, const int group_id);
  bool Slavenoone(const std::string& host, const int port, const int group_id);

  std::atomic<bool> running_;
  std::thread thread_;
  std::vector<std::vector<std::string>> groups_;
  std::vector<std::string> master_addrs_;
  std::unordered_map<std::string, int> count_;
  std::set<std::string> offline_nodes_;
  std::vector<int> term_ids_;
};

}  // namespace pikiwidb