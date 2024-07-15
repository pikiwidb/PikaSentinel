#include "ping_service.h"
#include "client.h"
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "etcd/Client.hpp"

namespace pikiwidb {

PingService::PingService()
        : etcd_("http://localhost:2379") {}

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

std::string PingService::DeCodeIp(std::string) {
  return "localhost";
}

int PingService::DeCodePort(std::string) {
  return 9221;
}


void PingService::AddHost(const pplx::task <etcd::Response> &message) {
  /*
   * 解析从 etcd 中拿到的 group message 信息提取出 ip-host 装载到 hosts_ 中
   */
  hosts_.emplace_back("localhost", 9221);
}

void PingService::SetClient(PClient *client) {
  client_ = client;
}

std::string PingService::EncodeIpPort(std::string server, int role) {
  return "ip:port:role";
}

/*
 * Pika Sentinel 线程启动，轮询给每个节点发送探活
 */
void PingService::Run() {
  while (running_) {
    // 从 etcd 中获取到 group 信息
    auto group_message = etcd_.get("/codis3/group");
    // 解析 message 信息装载到 hosts_ 中
    AddHost(group_message);
    for (const auto &[server, role]: hosts_) {
      // 提取出 host
      auto host = DeCodeIp(server);
      // 提取出 ip
      auto port = DeCodePort(server);
      // 探活
      bool result = PingRedis(host, port);
      auto iter = std::find(offline_nodes_.begin(), offline_nodes_.end(), server);
      // 如果探活成功并且发现节点在 offline_nodes_ 这个离线集合中，说明节点重新上线了
      if (result && iter != offline_nodes_.end()) {
        // 从 offline_nodes_ 离线节点集合中删除这个重新上线的节点
        offline_nodes_.erase(iter);
        // pika sentinel 向目标节点发送 slaveof 命令变更其节点信息
        if (Slaveof(host, port) && role == 1) {
          // 如果掉线节点之前是主节点，同步更新 etcd 中该节点的 role 角色, 变更为 slave 节点
          etcd_.set(host, "slave").wait();
        }
      }
      // 探活成功
      if (result) {
        continue;
      } else { // 如果探活失败，则进行计数
        // 组装 ip + port + role
        auto node = EncodeIpPort(server, role);
        count_[node]++;
        // 如果探活超过 10 次, 判断为下线
        if (count_[node] >= 10) {
          // 使用 offline_nodes_ 装载下线节点
          offline_nodes_.emplace_back(node);
          // 如果节点角色是 Master 节点, 那么就发生切主操作
          if (role == 0) {
            // 进行切主操作
            SwitchMaster();
          }
        }
      }
      std::this_thread::sleep_for(std::chrono::seconds(1)); // Sleep for 1 second between pings
    }
    std::this_thread::sleep_for(std::chrono::seconds(10)); // Sleep for 10 seconds between each full cycle
    // 清空 hosts_ 集群信息
    hosts_.clear();
  }
}

void PingService::SwitchMaster() {
  bool flag = false;
  for (const auto &[server, port]: hosts_) {
    if (std::find(offline_nodes_.begin(), offline_nodes_.end(), server) == offline_nodes_.end()) {
      // 提取出 host
      auto host = DeCodeIp(server);
      // 提取出 ip
      auto port = DeCodePort(server);
      // 选出新主节点，由 Pika sentinel 发送 slaveof 命令给目标节点
      if (Slavenoone(host, port)) {
        // 变更节点信息
        master_ip_ = host;
        master_port_ = port;
        // 同步更新 etcd 中该节点的角色
        etcd_.set(host, "master").wait();
        // 切主成功，将 flag 置为 true
        flag = true;
      }
      if (flag) {
        break;
      }
    }
  }
  // 对剩下的节点进行变更主节点操作
  for (const auto &[server, role]: hosts_) {
    if (std::find(offline_nodes_.begin(), offline_nodes_.end(), server) == offline_nodes_.end() && role == 0) {
      // 提取出 host
      auto host = DeCodeIp(server);
      // 提取出 ip
      auto port = DeCodePort(server);
      // 由 pika sentinel 发送 slaveof 命令给 目标节点变更其节点信息
      Slaveof(host, port);
    }
  }
}

bool PingService::PingRedis(const std::string &host, int port) {
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

  if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
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

bool PingService::Slaveof(const std::string &host, int port) {
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

  if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "Connection Failed" << std::endl;
    close(sock);
    return false;
  }

  std::string ping_cmd = "*3\r\n$7\r\nSlaveof\r\n";
  ping_cmd += master_ip_.size();
  ping_cmd += "\r\n";
  ping_cmd += master_ip_;
  ping_cmd += "\r\n";
  ping_cmd += master_port_.size();
  ping_cmd += "\r\n";
  ping_cmd += master_port_;
  ping_cmd += "\r\n";
  send(sock, ping_cmd.c_str(), ping_cmd.size(), 0);

  char reply[128];
  ssize_t reply_length = read(sock, reply, 128);
  std::string reply_str(reply, reply_length);

  close(sock);

  bool success = reply_str.find("+OK") != std::string::npos;
  return success;
}

bool PingService::Slavenoone(const std::string &host, int port) {
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

  if (connect(sock, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "Connection Failed" << std::endl;
    close(sock);
    return false;
  }

  std::string ping_cmd = "*3\r\n$7\r\nSlaveof\r\n$2\r\nno\r\n$3\r\none";
  send(sock, ping_cmd.c_str(), ping_cmd.size(), 0);
  char reply[128];
  ssize_t reply_length = read(sock, reply, 128);
  std::string reply_str(reply, reply_length);

  close(sock);

  bool success = reply_str.find("+OK") != std::string::npos;
  return success;
}

} // namespace pikiwidb