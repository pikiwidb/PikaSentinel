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
#include <json/json.h>

namespace pikiwidb {

    class PClient;

    struct InfoSlave {
        std::string ip;
        int port;
        int state;
        int offset; // 添加缺失的成员变量
    };

    struct InfoReplication {
        std::map<std::string, std::string> info;
        std::vector<InfoSlave> slaves;

        Json::Value toJson() const {
            Json::Value root;
            for (const auto& kv : info) {
                root["info"][kv.first] = kv.second;
            }

            for (const auto& slave : slaves) {
                Json::Value slaveJson;
                slaveJson["ip"] = slave.ip;
                slaveJson["port"] = slave.port;
                slaveJson["state"] = slave.state;
                slaveJson["offset"] = slave.offset; // 添加 offset 到 JSON
                root["slaves"].append(slaveJson);
            }

            return root;
        }

        std::string toStyledString() const {
            Json::StreamWriterBuilder writer;
            return Json::writeString(writer, toJson());
        }
    };


    class PKPingService {
    public:
        PKPingService();

        ~PKPingService();

        void Start();

        void Stop();

        void AddHost(const std::string &host, int port, int group_id, int term_id);

    private:
        void Run();

        std::string PKPingRedis(const std::string &host, int port, int group_id, int term_id, const std::string& msg);
        std::string sendRedisCommand(int sock, const std::string& command);
        InfoReplication parseInfoReplication(const std::string& info);
        std::atomic<bool> running_;
        std::thread thread_;
        std::vector<std::tuple<std::string, int, int, int>> hosts_;
        PClient *client_;
    };

}  // namespace pikiwidb