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

namespace pikiwidb {

    class PClient;

    class PingService {
    public:
        PingService();

        ~PingService();

        void Start();

        void Stop();

        void AddHost(const std::string &host, int port);

        void SetClient(PClient *client);

    private:
        void Run();

        bool PingRedis(const std::string &host, int port);

        std::atomic<bool> running_;
        std::thread thread_;
        std::vector<std::pair<std::string, int>> hosts_;
        PClient *client_;
    };

}  // namespace pikiwidb
