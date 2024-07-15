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
#include "etcd/Client.hpp"

namespace pikiwidb {

class PClient;

class PingService {
 public:
  PingService();
  ~PingService();
  void Start();
  void Stop();
  void SwitchMaster();
  void AddHost(const pplx::task <etcd::Response> &message);
  void SetClient(PClient *client);
  std::string EncodeIpPort(std::string server, int role);
  std::string DeCodeIp(std::string message);
  int DeCodePort(std::string message);

 private:
  void Run();
  bool PingRedis(const std::string &host, int port);
  bool Slaveof(const std::string &host, int port);
  bool Slavenoone(const std::string &host, int port);

  std::atomic<bool> running_;
  std::thread thread_;
  std::vector<std::pair<std::string, int>> hosts_;
  std::unordered_map<std::string, int> count_;
  std::vector<std::string> offline_nodes_;
  PClient *client_;
  std::string master_ip_;
  std::string master_port_;
  etcd::Client etcd_;
};

}  // namespace pikiwidb