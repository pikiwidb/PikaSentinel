#include "sentinel_service.h"
#include <iostream>
#include <cstring>
#include <net/redis_cli.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <curl/curl.h>
#include "httplib.h"

using json = nlohmann::json;
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

// json 序列化函数
void to_json(nlohmann::json& j, const Action& a) {
  j = nlohmann::json{{"index", a.index}, {"state", a.state}};
}

void to_json(nlohmann::json& j, const GroupServer& gs) {
  j = nlohmann::json {
          {"server", gs.addr},
          {"datacenter", gs.dataCenter},
          {"action", gs.action},
          {"role", gs.role},
          {"binlog_file_num", gs.db_binlog_filenum},
          {"binlog_offset", gs.db_binlog_offset},
          {"state", gs.state},
          {"recall_times", gs.recall_times},
          {"replica_group", gs.replica_group}
  };
}

void to_json(nlohmann::json& j, const Promoting& p) {
  j = nlohmann::json{{"index", p.index}, {"state", p.state}};
}

void to_json(nlohmann::json& j, const Group* g) {
  if (!g) {
    j = nullptr;
    return;
  }

  j = nlohmann::json {
          {"id", g->id},
          {"term_id", g->term_id},
          {"promoting", g->promoting},
          {"out_of_sync", g->out_of_sync}
  };

  j["servers"] = nlohmann::json::array();
  for (const auto& server : g->servers) {
    if (server) {
      j["servers"].push_back(*server);
    } else {
      j["servers"].push_back(nullptr);
    }
  }
}

void to_json(nlohmann::json& j, const GroupInfo& g) {
  j = nlohmann::json{
          {"groupid", g.groupid},
          {"termid", g.termid},
          {"masteraddr", g.masteraddr},
          {"slaveaddr", g.slaveaddr}
  };
}

// json 反序列化函数
void from_json(const json& j, Action& a) {
  if (j.contains("index")) {
    j.at("index").get_to(a.index);
  }
  if (j.contains("state")) {
    j.at("state").get_to(a.state);
  }
}

void from_json(const json& j, GroupServer& gs) {
  j.at("server").get_to(gs.addr);
  j.at("datacenter").get_to(gs.dataCenter);
  if (j.contains("action") && !j.at("action").is_null()) {
      j.at("action").get_to(gs.action);
  }
  j.at("role").get_to(gs.role);
  j.at("binlog_file_num").get_to(gs.db_binlog_filenum);
  j.at("binlog_offset").get_to(gs.db_binlog_offset);
  j.at("state").get_to(gs.state);
  j.at("recall_times").get_to(gs.recall_times);
  j.at("replica_group").get_to(gs.replica_group);
}

void from_json(const json& j, Promoting& p) {
  if (j.contains("index")) {
    j.at("index").get_to(p.index);
  }
  if (j.contains("state")) {
    j.at("state").get_to(p.state);
  }
}

void from_json(const json& j, Group& g) {
  j.at("id").get_to(g.id);
  j.at("term_id").get_to(g.term_id);
  if (j.contains("servers") && !j.at("servers").is_null()) {
    for (const auto& item : j.at("servers")) {
      auto* gs = new GroupServer;
      item.get_to(*gs);
      g.servers.push_back(gs);
    }
  }
  if (j.contains("promoting") && !j.at("promoting").is_null()) {
    j.at("promoting").get_to(g.promoting);
  }
  j.at("out_of_sync").get_to(g.out_of_sync);
}

void from_json(const nlohmann::json& j, GroupInfo& g) {
  j.at("groupid").get_to(g.groupid);
  j.at("termid").get_to(g.termid);
  j.at("masteraddr").get_to(g.masteraddr);
  j.at("slaveaddr").get_to(g.slaveaddr);
}

// 根据 addr 地址提取出 ip
std::string SentinelService::DeCodeIp(const std::string& serveraddr) {
  size_t pos = serveraddr.find(':');
  if (pos != std::string::npos) {
    return serveraddr.substr(0, pos);
  }
  return serveraddr;
}

// 根据 addr 地址提取出 port
int SentinelService::DeCodePort(const std::string& serveraddr) {
  size_t pos = serveraddr.find(':');
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
  return -1;
}

// HTTP GEt 回调函数，用于处理响应数据
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  ((std::string*)userp)->append((char*)contents, size * nmemb);
  return size * nmemb;
}

// HTTP Server 端
void SentinelService::HTTPServer() {
  httplib::Server svr;
  // 用于处理 dashboard 发来的删除一个 group 的 HTTP 请求
  svr.Post("/del", [this](const httplib::Request &req, httplib::Response &res) {
    auto json_data = req.body;
    try {
      nlohmann::json jsonData = nlohmann::json::parse(json_data);
      int index = jsonData.at("index").get<int>();
      if (index >= 0 && index < groups_.size()) {
        groups_.erase(groups_.begin() + index);
        res.set_content("Group deleted", "text/plain");
      } else {
        std::cerr << "Invalid index: " << index << std::endl;
        res.set_content("Invalid index", "text/plain");
      }
    } catch (json::parse_error& e) {
        std::cerr << "JSON parse error: " << e.what() << std::endl;
    } catch (json::type_error& e) {
        std::cerr << "JSON type error: " << e.what() << std::endl;
    }
    res.set_content("Update received", "text/plain");
  });

  // 用于处理 dashboard 发来的更新 group 信息的 HTTP 请求
  svr.Post("/update", [this](const httplib::Request &req, httplib::Response &res) {
      auto json_data = req.body;
      try {
        nlohmann::json jsonData = nlohmann::json::parse(json_data);
        int id = jsonData.at("id").get<int>();

        auto it = std::find_if(groups_.begin(), groups_.end(), [id](Group* group) {
          return group->id == id;
        });

        if (it != groups_.end()) {
          Group* group = *it;
          group->out_of_sync = jsonData.at("out_of_sync").get<bool>();
          group->term_id = jsonData.at("term_id").get<int>();
          group->promoting = jsonData.at("promoting").get<Promoting>();

          // 清空原有的 server 信息
          for (auto server : group->servers) {
            delete server;
          }
          group->servers.clear();

          // 更新 servers 信息
          for (const auto& server_json : jsonData.at("servers")) {
            auto server = new GroupServer();
            server_json.get_to(*server);
            group->servers.push_back(server);
          }
          res.set_content("Group updated", "text/plain");
        } else {
          std::cerr << "Group with id " << id << " not found" << std::endl;
          res.set_content("Group not found", "text/plain");
        }
      } catch (json::parse_error& e) {
          std::cerr << "JSON parse error: " << e.what() << std::endl;
      } catch (json::type_error& e) {
          std::cerr << "JSON type error: " << e.what() << std::endl;
      }
      res.set_content("Update received", "text/plain");
  });
  // HTTP-Server 监听 9225 端口
  std::cout << "Server listening on http://localhost:9225" << std::endl;
  svr.listen("0.0.0.0", 9225);
}

// HTTP 客户端
void SentinelService::HTTPClient() {
  CURL* curl;
  CURLcode res;
  std::string readBuffer;

  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl = curl_easy_init();
  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, "http://10.17.55.213:18080/topom/load-meta-data");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

    res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
    } else {
      // 获取到从 dashboard 拿到的数据装载到 readbuffer 中
      std::cout << "Response Data: " << readBuffer << std::endl;
    }
    // 解析从 dashboard 获取的 JSON 数据, 填充元信息到 groups_ 中
    try {
      json jsonData = json::parse(readBuffer);
      for (const auto& item : jsonData) {
        auto* g = new Group;
        item.get_to(*g);
        groups_.push_back(g);
      }
    } catch (json::parse_error& e) {
      std::cerr << "JSON parse error: " << e.what() << std::endl;
    } catch (json::type_error& e) {
      std::cerr << "JSON type error: " << e.what() << std::endl;
    }
    curl_easy_cleanup(curl);
  }
  curl_global_cleanup();
}

bool SentinelService::IsGroupMaster(ReplicationState* state, Group* group) {
  return state->index == 0 && group->servers[0]->addr == state->addr;
}

Group* SentinelService::GetGroup(int gid) {
  return groups_[gid];
}

/*
 * 对 state 状态值进行判断, 更新 server 节点的信息
 */
void SentinelService::CheckAndUpdateGroupServerState(GroupServer* server, ReplicationState* state, Group* group) {
  // 如果 err 值为 true，说明没有存活
  if (!state->err) {
    if (server->state == static_cast<int8_t>(GroupState::GroupServerStateNormal)) {
      // 节点主观下线
      server->state = static_cast<int8_t>(GroupState::GroupServerStateSubjectiveOffline);
    } else {
      server->recall_times++;
      // 如果累加到 10 次还是未存活
      if (server->recall_times >= 10) {
        // 节点客观下线，更新元信息
        server->state = static_cast<int8_t>(GroupState::GroupServerStateOffline);
        server->action.state = ActionState::Nothing;
        server->replica_group = false;
      }
      // 如果节点已经客观下线，并且节点是 master 节点，则放入 master_offline_groups 队列
      if (server->state == static_cast<int8_t>(GroupState::GroupServerStateOffline) && IsGroupMaster(state, group)) {
        master_offline_groups_.emplace_back(group);
      } else {
        // 否则放入 slave_offline_groups 队列
        slave_offline_groups_.emplace_back(group);
      }
    }
  } else {
    // 如果节点之前是客观下线状态，但是这次 pkping 是存活状态，说明节点重新上线了，放入 recover_groups 队列
    if (server->state == static_cast<int8_t>(GroupState::GroupServerStateOffline)) {
      recovered_groups_.emplace_back(state);
    } else {
      // 探活正常，重置 server 节点的元信息
      server->state = static_cast<int8_t>(GroupState::GroupServerStateNormal);
      server->recall_times = 0;
      server->replica_group = true;
      server->role = state->replication.role;
      server->db_binlog_filenum = state->replication.db_binlog_filenum;
      server->db_binlog_offset = state->replication.db_binlog_offset;
      server->action.state = ActionState::Synced;
    }
  }
}

// 向 dashboard 发送 HTTP Post 请求变更 etcd 元信息
void SentinelService::UpdateGroup(Group* group) {
  nlohmann::json json_group = group;
  curl_global_init(CURL_GLOBAL_ALL);
  CURL* curl = curl_easy_init();
  if (curl) {
    std::string json_data = json_group.dump(4);
    curl_easy_setopt(curl, CURLOPT_URL, "http://10.17.55.213:18080/topom/upload-meta-data");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data.c_str());
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
      std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
    } else {
      std::cout << "POST request sent successfully." << std::endl;
    }
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  curl_global_cleanup();
}

void SentinelService::UpdateSlaveOfflineGroups() {
  for (auto& group : slave_offline_groups_) {
    // 更新 group 中的 out_of_sync 值，向 dashboard 发送 HTTP Post 请求，变更 etcd 信息
    group->out_of_sync = true;
    UpdateGroup(group);
  }
}

void SentinelService::SelectNewMaster(Group* group, std::string& newMasterAddr, int newMasterIndex) {
  // 通过 filnume 和 offset 判断哪个节点的数据最新，选取新的主节点
  for (int index = 0; index < group->servers.size(); ++index) {
    if (index == 0 || group->servers[index]->state != static_cast<int8_t>(GroupState::GroupServerStateNormal)) {
      continue;
    }
    if (newMasterServer_ == nullptr) {
      newMasterServer_ =  group->servers[index];
      newMasterIndex = index;
    } else if (group->servers[index]->db_binlog_filenum > newMasterServer_->db_binlog_filenum) {
      newMasterServer_ = group->servers[index];
      newMasterIndex = index;
    } else if (group->servers[index]->db_binlog_filenum == newMasterServer_->db_binlog_filenum) {
      if (group->servers[index]->db_binlog_offset > newMasterServer_->db_binlog_offset) {
        newMasterServer_ = group->servers[index];
        newMasterIndex = index;
      }
    }
  }
  if (newMasterServer_ == nullptr) {
    newMasterAddr = "";
  }
  // 用 newMasterAddr 存取新的主节点的 addr
  newMasterAddr = newMasterServer_->addr;
}

void SentinelService::DoSwitchGroupMaster(pikiwidb::Group *group, std::string& newMasterAddr, int newMasterIndex) {
  if (newMasterIndex <= 0 || newMasterAddr.empty()) {
    return;
  }
  // 对新的主节点发送 slaveof no one 命令，并且变更其元信息
  Slavenoone(newMasterAddr);
  group->servers[newMasterIndex]->role = GroupServerRoleStrings::Master;
  group->servers[newMasterIndex]->action.state = ActionState::Synced;
  // 将新主节点在 groups_ 中 server 的位置移到第一行，因为 master 节点都存取在 json 的第一行
  std::swap(group->servers[0], group->servers[newMasterIndex]);
  // group 的 term-id 发生自增
  group->term_id++;
  // 向 dashboard 发送 HTTP Post 请求变更 etcd 元信息
  UpdateGroup(group);
  // 对剩余的从节点发送 slaveof 命令，变更到新的主节点上
  for (auto& server : group->servers) {
    if (server->state != static_cast<int8_t>(GroupState::GroupServerStateNormal) || server->addr == newMasterAddr) {
      continue;
    }
    if (Slaveof(server->addr, newMasterAddr)) {
      server->action.state =  ActionState::SyncedFailed;
      server->state = static_cast<int8_t>(GroupState::GroupServerStateOffline);
    } else {
      server->action.state = ActionState::Synced;
      server->role = GroupServerRoleStrings::Slave;
    }
  }
}

void SentinelService::TrySwitchGroupMaster(Group* group) {
  std::string newMasterAddr;
  int newMasterIndex = -1;
  // 选取新的主节点
  SelectNewMaster(group, newMasterAddr, newMasterIndex);
  // 切换新的主节点
  DoSwitchGroupMaster(group, newMasterAddr, newMasterIndex);
}

void SentinelService::TrySwitchGroupsToNewMaster() {
  for (auto& group : master_offline_groups_) {
    group->out_of_sync = true;
    // 变更 group 的 out_of_sync 信息，向 dashboard 发送 HTTP Post 请求, 变更 etcd 元信息
    UpdateGroup(group);
    TrySwitchGroupMaster(group);
  }
}

std::string JoinHostPost(std::string& master_host, std::string& master_port) {
  return master_host + ":" + master_port;
}

std::string SentinelService::GetMasterAddr(std::string& master_host, std::string& master_port) {
  if (master_host.empty()) {
    return "";
  }
  return JoinHostPost(master_host, master_port);
}

void SentinelService::TryFixReplicationRelationship(Group *group, GroupServer *server,
                                                    ReplicationState *state, int masterofflinegroups) {
  std::string curMasterAddr = group->servers[0]->addr;
  if (IsGroupMaster(state, group)) {
    // 如果掉线节点之前是主节点，并且很快就恢复，则不用进行处理
    if (state->replication.role == GroupServerRoleStrings::Master) {
      return;
    }
    // 如果掉线节点之前是主节点，并且离线时间较长，则需要重新 slaveof 新的主节点
    Slavenoone(state->addr);
  } else {
    // 如果掉线节点之前是从节点，在掉线期间没有新的主从关系产生，那么还是保持和原来的状态一致
    if (GetMasterAddr(state->replication.maste_host, state->replication.master_port) == curMasterAddr) {
      return;
    }
    // 如果掉线节点之前是从节点，在掉线期间有新的主从关系产生，那么需要重新 slaveof 新的主节点
    Slaveof(server->addr, curMasterAddr);
  }
  // 重置 server 节点的元信息
  server->state = static_cast<int8_t>(GroupState::GroupServerStateNormal);
  server->recall_times = 0;
  server->replica_group = true;
  server->role = state->replication.role;
  server->db_binlog_filenum = state->replication.db_binlog_filenum;
  server->db_binlog_offset = state->replication.db_binlog_offset;
  server->action.state = ActionState::Synced;
  // 向 dashboard 发送 HTTP Post 请求变更 etcd 元信息
  UpdateGroup(group);
}

void SentinelService::TryFixReplicationRelationships(int masterOfflineGroups) {
  for (auto& state : recovered_groups_) {
    auto group = GetGroup(state->group_id);
    group->out_of_sync = true;
    // 变更 group 的 out_of_sync 信息，向 dashboard 发送 HTTP Post 请求, 变更 etcd 元信息
    UpdateGroup(group);
    // 由于掉线节点在离线i期间可能有新的主从关系的变更，这里进行这部分的处理
    TryFixReplicationRelationship(group, state->server, state, masterOfflineGroups);
  }
}

void SentinelService::RefreshMastersAndSlavesClientWithPKPing() {
  for (auto& group: groups_) {
    GroupInfo group_info;
    group_info.groupid = group->id;
    group_info.termid = group->term_id;
    for (auto &server: group->servers) {
      if (server->role == GroupServerRoleStrings::Master) {
        group_info.masteraddr.push_back(server->addr);
      }
      if (server->role == GroupServerRoleStrings::Slave) {
        group_info.slaveaddr.push_back(server->addr);
      }
    }
    for (auto &server: group->servers) {
      nlohmann::json json_groupInfo = group_info;
      PKPingRedis(server->addr, json_groupInfo);
    }
  }
}

void SentinelService::CheckMastersAndSlavesState() {
  // to do @chejinge
  // 发送 PKPing 命令进行探活
  // RefreshMastersAndSlavesClientWithPKPing();

  // 对每一个节点的状态值进行遍历，查看是否存活
  for (auto& state : states_) {
    auto group = GetGroup(state->group_id);
    CheckAndUpdateGroupServerState(state->server, state, group);
  }
  if (!slave_offline_groups_.empty()) {
    // 对客观下线的从节点进行处理
    UpdateSlaveOfflineGroups();
  }
  if (!master_offline_groups_.empty()) {
    // 对客观下线的主节点进行处理
    TrySwitchGroupsToNewMaster();
  }
  if (!recovered_groups_.empty()) {
    // 对之前下线过又重新上线的节点进行处理
    TryFixReplicationRelationships(master_offline_groups_.size());
  }
}

/*
 * Pika Sentinel 线程启动
 */
void SentinelService::Run() {
  // 启动 HTTP-Server 线程
  std::thread server_thread(&SentinelService::HTTPServer, this);
  // 启动 HTTP-Client
  HTTPClient();
  while (running_) {
    // 每 10 秒检查一次主从状态
    CheckMastersAndSlavesState();
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }
  // 等待 HTTP-Server 线程结束
  server_thread.join();
}

// Pkping 命令
void SentinelService::PKPingRedis(std::string& addr, nlohmann::json jsondata) {
  auto host = DeCodeIp(addr);
  auto port = DeCodePort(addr);
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    std::cerr << "Socket creation error" << std::endl;
  }

  struct sockaddr_in serv_addr{};
  memset(&serv_addr, 0, sizeof(serv_addr));

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(port);

  if (inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr) <= 0) {
    std::cerr << "Invalid address/ Address not supported" << std::endl;
    close(sock);
  }

  if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    std::cerr << "Connection Failed" << std::endl;
    close(sock);
  }
  std::string cmd;
  net::RedisCmdArgsType argv;
  argv.push_back("PkPing");
  argv.push_back(jsondata.dump());
  net::SerializeRedisCommand(argv, &cmd);
  send(sock, cmd.c_str(), cmd.size(), 0);

  char reply[128];
  ssize_t reply_length = read(sock, reply, 128);
  std::string reply_str(reply, reply_length);

  close(sock);
  std::cout << "reply: " << reply_str << std::endl;
}

// slaveof 命令
bool SentinelService::Slaveof(const std::string& addr, std::string& newMasterAddr) {
  auto master_ip = DeCodeIp(newMasterAddr);
  auto master_port = DeCodePort(newMasterAddr);
  auto host = DeCodeIp(addr);
  auto port = DeCodePort(addr);
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    std::cerr << "Socket creation error" << std::endl;
    return false;
  }

  struct sockaddr_in serv_addr{};
  memset(&serv_addr, 0, sizeof(serv_addr));

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
  std::string cmd;
  net::RedisCmdArgsType argv;
  argv.push_back("Slaveof");
  argv.push_back(master_ip);
  argv.push_back(std::to_string(master_port));
  net::SerializeRedisCommand(argv, &cmd);
  send(sock, cmd.c_str(), cmd.size(), 0);

  char reply[128];
  ssize_t reply_length = read(sock, reply, 128);
  std::string reply_str(reply, reply_length);
  close(sock);
  bool success = reply_str.find("+OK") != std::string::npos;
  return success;
}

// slaveof no one 命令
bool SentinelService::Slavenoone(const std::string& addr) {
  auto host = DeCodeIp(addr);
  auto port = DeCodePort(addr);
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    std::cerr << "Socket creation error" << std::endl;
    return false;
  }

  struct sockaddr_in serv_addr{};
  memset(&serv_addr, 0, sizeof(serv_addr));

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
  std::string cmd;
  net::RedisCmdArgsType argv;
  argv.push_back("Slaveof");
  argv.push_back("no");
  argv.push_back("one");
  net::SerializeRedisCommand(argv, &cmd);
  send(sock, cmd.c_str(), cmd.size(), 0);
  char reply[128];
  ssize_t reply_length = read(sock, reply, 128);
  std::string reply_str(reply, reply_length);

  close(sock);

  bool success = reply_str.find("+OK") != std::string::npos;
  return success;
}

} // namespace pikiwidb