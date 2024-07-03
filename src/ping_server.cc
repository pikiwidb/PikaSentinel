#include "ping_service.h"
#include "client.h"
#include <chrono>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace pikiwidb {

    PingService::PingService() : running_(false), client_(nullptr) {}

    PingService::~PingService() {
        Stop();
    }

    void PingService::Start() {
        running_ = true;
        thread_ = std::thread(&PingService::Run, this);
    }

    void PingService::Stop() {
        running_ = false;
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    void PingService::AddHost(const std::string& host, int port) {
        hosts_.emplace_back(host, port);
    }

    void PingService::SetClient(PClient* client) {
        client_ = client;
    }

    void PingService::Run() {
        while (running_) {
            for (const auto& [host, port] : hosts_) {
                bool result = PingRedis(host, port);
                if (client_) {
                    std::string msg = result ? "PONG" : "FAIL";
                    client_->GetTcpConnection()->SendPacket(msg.data(), msg.size());
                }
                std::this_thread::sleep_for(std::chrono::seconds(1)); // Sleep for 1 second between pings
            }
            std::this_thread::sleep_for(std::chrono::seconds(10)); // Sleep for 10 seconds between each full cycle
        }
    }

    bool PingService::PingRedis(const std::string& host, int port) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << "Socket creation error" << std::endl;
            return false;
        }

        struct sockaddr_in serv_addr;
        memset(&serv_addr, '0', sizeof(serv_addr));

        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(port);

        if (inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr) <= 0) {
            std::cerr << "Invalid address/ Address not supported" << std::endl;
            close(sock);
            return false;
        }

        if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "Connection Failed" << std::endl;
            close(sock);
            return false;
        }

        std::string ping_cmd = "*1\r\n$4\r\nPING\r\n";
        send(sock, ping_cmd.c_str(), ping_cmd.size(), 0);

        char reply[128];
        ssize_t reply_length = read(sock, reply, 128);
        std::string reply_str(reply, reply_length);

        close(sock);

        bool success = reply_str.find("+PONG") != std::string::npos;

        // Log the result
        if (success) {
            std::cout << "Ping to " << host << ":" << port << " succeeded." << std::endl;
        } else {
            std::cout << "Ping to " << host << ":" << port << " failed." << std::endl;
        }

        return success;
    }

}  // namespace pikiwidb