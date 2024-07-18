#include "sentinel_service.h"
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <nghttp2/nghttp2.h>
namespace pikiwidb {

SentinelService::SentinelService() {

}

SentinelService::~SentinelService() {
  Stop();
}

void SentinelService::Start() {
  running_ = true;
  thread_ = std::thread(&SentinelService::Run, this);
}

void SentinelService::Stop() {
  running_ = false;
  if (thread_.joinable()) {
    thread_.join();
  }
}

std::string SentinelService::DeCodeIp(const std::string& serveraddr) {
  size_t pos = serveraddr.find(':');
  // 如果找到分隔符，提取IP地址部分
  if (pos != std::string::npos) {
    return serveraddr.substr(0, pos);
  }
  // 如果没有找到分隔符，返回整个字符串
  return serveraddr;
}

int SentinelService::DeCodePort(const std::string& serveraddr) {
  // 查找分隔符 ':'
  size_t pos = serveraddr.find(':');

  // 如果找到分隔符，提取端口号部分并转换为整数
  if (pos != std::string::npos) {
    std::string portStr = serveraddr.substr(pos + 1);
    try {
      int port = std::stoi(portStr);
      return port;
    } catch (const std::invalid_argument& e) {
      std::cerr << "Invalid port number: " << portStr << std::endl;
    } catch (const std::out_of_range& e) {
      std::cerr << "Port number out of range: " << portStr << std::endl;
    }
  }
  // 如果没有找到分隔符，返回一个默认值或错误值
  return -1;
}

//void SentinelService::LoadMeta(const std::string& message) {
//  // 解析 JSON 字符串
//  json jsonObj = json::parse(jsonString);
//
//  // 遍历 servers 数组，提取每个 server 字段
//  for (const auto& serverObj : jsonObj["servers"]) {
//    groups_[1].push_back(serverObj["server"]);
//  }
//}

void SentinelService::RequestData() {

}

/*
 * Pika Sentinel 线程启动, 轮询给每个节点发送探活
 */
void SentinelService::Run() {
  RequestData();
  while (running_) {
    int group_id = 0;
    for (const auto& group: groups_) {
      group_id++;
      for (const auto& serveraddr: group) {
        // 提取出 host
        auto host = DeCodeIp(serveraddr);
        // 提取出 ip
        auto port = DeCodePort(serveraddr);
        // 探活
        bool result = PKPingRedis(host, port, group_id);
        auto node = std::find(group.begin(), group.end(), serveraddr);
        // 查找该节点是否在离线节点中
        auto iter = std::find(offline_nodes_.begin(), offline_nodes_.end(), serveraddr);
        // 如果探活成功并且发现节点在 offline_nodes_ 这个离线集合中，说明节点重新上线了
        if (result && iter != offline_nodes_.end()) {
          // 从 offline_nodes_ 离线节点集合中删除这个重新上线的节点
          offline_nodes_.erase(iter);
          // pika sentinel 向目标节点发送 slaveof 命令变更其节点信息
          Slaveof(host, port, group_id);
        }
        // 探活成功
        if (result) {
          continue;
        } else { // 如果探活失败，则进行计数
          // 组装 ip + port + role
          count_[serveraddr]++;
          // 如果探活超过 10 次, 判断为下线
          if (count_[serveraddr] >= 10) {
            // 使用 offline_nodes_ 装载下线节点，计数器重置
            count_[serveraddr] = 0;
            offline_nodes_.insert(serveraddr);
            // 如果节点角色是 Master 节点, 那么就发生切主操作
            if (static_cast<int>(std::distance(node, group.begin()) == 0)) {
              term_ids_[group_id]++;
              // 进行切主操作
              SwitchMaster(group_id);
            }
          }
        }
        std::this_thread::sleep_for(std::chrono::seconds(1)); // Sleep for 1 second between pings
      }
      std::this_thread::sleep_for(std::chrono::seconds(2)); // Sleep for 10 seconds between each full cycle
    }
  }
}

void SentinelService::SwitchMaster(const int group_id) {
  bool flag = false;
  for (const auto &addr: groups_[group_id]) {
    if (std::find(offline_nodes_.begin(), offline_nodes_.end(), addr) == offline_nodes_.end()) {
      // 提取出 host
      auto host = DeCodeIp(addr);
      // 提取出 ip
      auto port = DeCodePort(addr);
      // 选出新主节点，由 Pika sentinel 发送 slaveofonone 命令给目标节点
      if (Slavenoone(host, port, group_id)) {
        // 变更节点信息
        master_addrs_[group_id] = addr;
        flag = true;
      }
    }
    if (flag) {
      break;
    }
  }
  // 对剩下的节点进行变更主节点操作
  for (const auto &addr: groups_[group_id]) {
    if (std::find(offline_nodes_.begin(), offline_nodes_.end(), addr) == offline_nodes_.end() && addr != master_addrs_[group_id]) {
      // 提取出 host
      auto host = DeCodeIp(addr);
      // 提取出 ip
      auto port = DeCodePort(addr);
      // 由 pika sentinel 发送 slaveof 命令给 目标节点变更其节点信息
      Slaveof(host, port, group_id);
    }
  }
}

bool SentinelService::PKPingRedis(const std::string& host, const int port, const int group_id) {
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


  // Prepare PKPing command with necessary parameters
  std::string pkping_cmd = "*4\r\n$6\r\nPKPING\r\n";
  pkping_cmd += "$" + std::to_string(std::to_string(group_id).length()) + "\r\n" + std::to_string(group_id) + "\r\n";
  pkping_cmd += "$" + std::to_string(std::to_string(term_ids_[group_id]).length()) + "\r\n" + std::to_string(term_ids_[group_id]) + "\r\n";

  send(sock, pkping_cmd.c_str(), pkping_cmd.size(), 0);

  char reply[128];
  ssize_t reply_length = read(sock, reply, 128);
  std::string reply_str(reply, reply_length);

  close(sock);

  bool success = reply_str.find("+PONG") != std::string::npos;

  // Log the result
  if (success) {
    std::cout << "PKPing to " << host << ":" << port << " succeeded." << std::endl;
  } else {
    std::cout << "PKPing to " << host << ":" << port << " failed." << std::endl;
  }
  return success;
}

bool SentinelService::PingRedis(const std::string &host, int port) {
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

bool SentinelService::Slaveof(const std::string& host, const int port, const int group_id) {
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
  ping_cmd += DeCodeIp(master_addrs_[group_id]).size();
  ping_cmd += "\r\n";
  ping_cmd += DeCodeIp(master_addrs_[group_id]);
  ping_cmd += "\r\n";
  ping_cmd += std::to_string(DeCodePort(master_addrs_[group_id])).size();
  ping_cmd += "\r\n";
  ping_cmd += DeCodePort(master_addrs_[group_id]);
  ping_cmd += "\r\n";
  send(sock, ping_cmd.c_str(), ping_cmd.size(), 0);

  char reply[128];
  ssize_t reply_length = read(sock, reply, 128);
  std::string reply_str(reply, reply_length);

  close(sock);

  bool success = reply_str.find("+OK") != std::string::npos;
  return success;
}

bool SentinelService::Slavenoone(const std::string& host, const int port, const int group_id) {
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