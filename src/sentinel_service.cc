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

static bool Slavenoone(const std::string& addr);
static bool Slaveof(const std::string& addr, const std::string& newMasterAddr);

SentinelService::SentinelService() = default;

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

void parseInfoReplication(const std::string& data, InfoReplication& info) {
  size_t pos = data.find("\r\n");
  if (pos != std::string::npos) {
    std::string trimmedData = data.substr(pos + 2); // Skip past the "$221\r\n"
    std::istringstream ss(trimmedData);
    std::string line;
    while (std::getline(ss, line)) {
      if (line.find("role:") == 0) {
        info.role = line.substr(5);
      } else if (line.find("connected_slaves:") == 0) {
        info.connected_slaves = std::stoi(line.substr(17));
      } else if (line.find("slave") == 0 && info.role == "master") {
        InfoSlave slave;
        size_t ipPos = line.find("ip=");
        size_t portPos = line.find("port=");
        size_t connFdPos = line.find("conn_fd=");
        size_t lagPos = line.find("lag=");

        if (ipPos != std::string::npos && portPos != std::string::npos) {
          slave.ip = line.substr(ipPos + 3, line.find(',', ipPos) - ipPos - 3);
          slave.port = line.substr(portPos + 5, line.find(',', portPos) - portPos - 5);
        }

        if (lagPos != std::string::npos) {
          size_t dbPos = line.find("db0:", lagPos);
          if (dbPos != std::string::npos) {
            slave.offset = std::stoi(line.substr(dbPos + 4));
          }
        }

        info.slaves.push_back(slave);
      } else if (line.find("db0:binlog_offset=") == 0) {
        std::istringstream binlogStream(line.substr(18));
        binlogStream >> info.db_binlog_filenum;
        binlogStream.ignore(1, ' ');
        binlogStream >> info.db_binlog_offset;
      } else if (line.find("master_host:") == 0) {
        info.master_host = line.substr(12);
      } else if (line.find("master_port:") == 0) {
        info.master_port = line.substr(12);
      } else if (line.find("master_link_status:") == 0) {
        info.master_link_status = line.substr(19);
      }
    }
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
  j = json::object();
  j = nlohmann::json{
          {"group_id", g.group_id},
          {"term_id", g.term_id},
          {"masters_addr", g.masters_addr},
          {"slaves_addr", g.slaves_addr},
          {"sentienl_addr"}, g.sentinel_addr};
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
  j.at("group_id").get_to(g.group_id);
  j.at("term_id").get_to(g.term_id);
  j.at("masters_addr").get_to(g.masters_addr);
  j.at("slaves_addr").get_to(g.slaves_addr);
  j.at("sentinel_addr").get_to(g.sentinel_addr);
}

// 根据 addr 地址提取出 ip
static std::string DeCodeIp(const std::string& serveraddr) {
  size_t pos = serveraddr.find(':');
  if (pos != std::string::npos) {
    return serveraddr.substr(0, pos);
  }
  return serveraddr;
}

// 根据 addr 地址提取出 port
static int DeCodePort(const std::string& serveraddr) {
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

// HTTP GET 回调函数，用于处理响应数据
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
      std::lock_guard<std::mutex> lock(groups_mtx_);
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
      std::lock_guard<std::mutex> lock(groups_mtx_);
      auto it = std::find_if(groups_.begin(), groups_.end(), [id](Group* group) {
        return group->id == id;
      });
      Group* group = nullptr;
      if (it != groups_.end()) {
        group = *it;
        for (auto server : group->servers) {
          delete server;
        }
        group->servers.clear();
      } else {
        group = new Group();
        group->id = id;
        groups_.push_back(group);
      }
      group->out_of_sync = jsonData.at("out_of_sync").get<bool>();
      group->term_id = jsonData.at("term_id").get<int>();
      group->promoting = jsonData.at("promoting").get<Promoting>();
      // 更新 servers 信息
      for (const auto& server_json : jsonData.at("servers")) {
        auto server = new GroupServer();
        server_json.get_to(*server);
        group->servers.push_back(server);
      }
      res.set_content("Group updated", "text/plain");
    } catch (json::parse_error& e) {
      std::cerr << "JSON parse error: " << e.what() << std::endl;
      res.set_content("JSON parse error", "text/plain");
    } catch (json::type_error& e) {
      std::cerr << "JSON type error: " << e.what() << std::endl;
      res.set_content("JSON type error", "text/plain");
    }
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

static bool IsGroupMaster(ReplicationState* state, Group* group) {
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
  if (state->err) {
    if (server->state == static_cast<int8_t>(GroupState::GroupServerStateNormal)) {
      // 节点主观下线
      server->state = static_cast<int8_t>(GroupState::GroupServerStateSubjectiveOffline);
    } else {
      // 探活失败计数
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
static void UpdateGroup(Group* group) {
  nlohmann::json json_group = group;
  curl_global_init(CURL_GLOBAL_ALL);
  CURL* curl = curl_easy_init();
  if (curl) {
    std::string response_string;
    std::string json_data = json_group.dump(4);
    curl_easy_setopt(curl, CURLOPT_URL, "http://10.17.55.213:18080/topom/upload-meta-data");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data.c_str());
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(curl);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_string);
    if (res != CURLE_OK) {
      std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
    } else {
      if (response_string.find("OK") != std::string::npos) {
        std::cout << "POST request sent successfully and received 'ok' in the response." << std::endl;
      } else {
        std::cerr << response_string << std::endl;
      }
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

static void SelectNewMaster(Group* group, std::string& newMasterAddr, int& newMasterIndex) {
  GroupServer* newMasterServer = nullptr; // 新的主节点
  // 通过 filnume 和 offset 判断哪个节点的数据最新，选取新的主节点
  for (int index = 0; index < group->servers.size(); ++index) {
    // 如果 index 等于 0, 并且还是客观下线状态，说明当前节点就是掉线的旧主节点，我们可以跳过
    if (index == 0 || group->servers[index]->state != static_cast<int8_t>(GroupState::GroupServerStateNormal)) {
      continue;
    }
    if (newMasterServer == nullptr) {
      newMasterServer =  group->servers[index];
      newMasterIndex = index;
      // filenum 更大的数据最新
    } else if (group->servers[index]->db_binlog_filenum > newMasterServer->db_binlog_filenum) {
      newMasterServer = group->servers[index];
      newMasterIndex = index;
      // filenum 一样大的, offset 的数值越大的数据越新
    } else if (group->servers[index]->db_binlog_filenum == newMasterServer->db_binlog_filenum) {
      if (group->servers[index]->db_binlog_offset > newMasterServer->db_binlog_offset) {
        newMasterServer = group->servers[index];
        newMasterIndex = index;
      }
    }
  }
  if (newMasterServer == nullptr) {
    newMasterAddr = "";
  }
  // 用 newMasterAddr 存取新的主节点的 addr
  newMasterAddr = newMasterServer->addr;
}

static bool DoSwitchGroupMaster(pikiwidb::Group *group, const std::string& newMasterAddr, const int newMasterIndex) {
  if (newMasterIndex <= 0 || newMasterAddr.empty()) {
    return true;
  }
  // 对新的主节点发送 slaveof no one 命令，并且变更其元信息
  if (Slavenoone(newMasterAddr)) {
    std::cerr << "promote serve " << newMasterAddr << "to new master failed" << std::endl;
    return false;
  }
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
    // 如果 Slaveof 失败, 则状态更改为 SyncedFailed 状态
    if (!Slaveof(server->addr, newMasterAddr)) {
      std::cerr << "group " << group->id << "update server" << newMasterIndex << "replication relationship failed, new master: " << newMasterAddr;
      server->action.state = ActionState::SyncedFailed;
      server->state = static_cast<int8_t>(GroupState::GroupServerStateOffline);
    } else {
      server->action.state = ActionState::Synced;
      server->role = GroupServerRoleStrings::Slave;
    }
  }
  return true;
}

static bool TrySwitchGroupMaster(Group* group) {
  std::string newMasterAddr;
  int newMasterIndex = -1;
  // 选取新的主节点
  SelectNewMaster(group, newMasterAddr, newMasterIndex);
  if (newMasterAddr.empty()) {
    std::cerr <<  "group " << group->id << "don't has any slaves to switch master" << std::endl;
    return false;
  }
  // 切换新的主节点
  return DoSwitchGroupMaster(group, newMasterAddr, newMasterIndex);
}

void SentinelService::TrySwitchGroupsToNewMaster() {
  for (auto& group : master_offline_groups_) {
    group->out_of_sync = true;
    // 变更 group 的 out_of_sync 信息，向 dashboard 发送 HTTP Post 请求, 变更 etcd 元信息
    UpdateGroup(group);
    if (!TrySwitchGroupMaster(group)) {
      std::cerr << "group-" << group->id << "switch master failed" << std::endl;
    }
  }
}

std::string JoinHostPost(const std::string& master_host, const std::string& master_port) {
  return master_host + ":" + master_port;
}

static std::string GetMasterAddr(const std::string& master_host, const std::string& master_port) {
  if (master_host.empty()) {
    return "";
  }
  return JoinHostPost(master_host, master_port);
}

static bool TryFixReplicationRelationship(Group *group, GroupServer *server,
                                                    ReplicationState *state, const size_t master_offline_groups) {
  std::string curMasterAddr = group->servers[0]->addr;
  if (IsGroupMaster(state, group)) {
    // 如果当前节点是 master 节点，并且主节点客观下线集合中有值，说明不需要处理
    if (state->replication.role == GroupServerRoleStrings::Master) {
      if (master_offline_groups > 0) {
        return true;
      }
    }
    // 如果掉线节点之前是主节点，并且离线时间较长，则需要重新 slaveof 新的主节点
    if (!Slavenoone(state->addr)) {
      return false;
    }
  } else {
    // 如果掉线节点之前是从节点，在掉线期间没有新的主从关系产生，那么还是保持和原来的状态一致
    if (GetMasterAddr(state->replication.master_host, state->replication.master_port) == curMasterAddr) {
      return true;
    }
    // 如果掉线节点之前是从节点，在掉线期间有新的主从关系产生，那么需要重新 slaveof 新的主节点
    if (!Slaveof(server->addr, curMasterAddr)) {
      return false;
    }
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
  return true;
}

void SentinelService::TryFixReplicationRelationships(const size_t masterOfflineGroups) {
  for (auto& state : recovered_groups_) {
    auto group = GetGroup(state->group_id);
    group->out_of_sync = true;
    // 变更 group 的 out_of_sync 信息，向 dashboard 发送 HTTP Post 请求, 变更 etcd 元信息
    UpdateGroup(group);
    // 由于掉线节点在离线i期间可能有新的主从关系的变更，这里进行这部分的处理
    if (TryFixReplicationRelationship(group, state->server, state, masterOfflineGroups)) {
      std::cerr << "group-[" << group->id << "]fix serve[" << state->addr << "] replication relationship failed" << std::endl;
    }
  }
}

void SentinelService::RefreshMastersAndSlavesClientWithPKPing() {
  if (groups_.empty()) {
    std::cerr << "There's no groups" << std::endl;
    return;
  }
  std::map<int, int> groups_info;
  // 建立 gid 和 term-id 的映射关系
  for (auto& group : groups_) {
    groups_info[group->id] = group->term_id;
  }
  // 因为在同一个 Group 里面的节点，向它们发送的 group_info 肯定是一样的，所以用 map 存储
  std::map<int, GroupInfo> groups_parameter;
  // 组装 PkPing 命令的 GroupInfo 信息
  for (auto& group : groups_) {
    GroupInfo group_info;
    group_info.group_id = group->id;
    group_info.term_id = groups_info[group->id];
    group_info.sentinel_addr = sentinel_addr_;
    for (auto &server: group->servers) {
      if (server->role == GroupServerRoleStrings::Master) {
        group_info.masters_addr.push_back(server->addr);
      }
      if (server->role == GroupServerRoleStrings::Slave) {
        group_info.slaves_addr.push_back(server->addr);
      }
    }
    groups_parameter[group->id] = group_info;
  }
  for (auto& group : groups_) {
    nlohmann::json json_groupInfo = groups_parameter[group->id];
    for (int index = 0; index < group->servers.size(); ++index) {
      auto state = new ReplicationState();
      state->addr = group->servers[index]->addr;
      state->server = group->servers[index];
      state->group_id = group->id;
      state->err = false;
      state->index = index;
      // 发送 PkPing 命令给目标节点
      PKPingRedis(group->servers[index]->addr, json_groupInfo, state);
    }
  }
}

void SentinelService::CheckMastersAndSlavesState() {
  // 探活发送 PkPing 命令
  RefreshMastersAndSlavesClientWithPKPing();
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

// PKPing 命令
void SentinelService::PKPingRedis(const std::string& addr, const nlohmann::json& jsondata, ReplicationState* state) {
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
  std::string group_info = jsondata.dump();
  net::RedisCmdArgsType argv;
  argv.emplace_back("pkping");
  argv.emplace_back(group_info);
  net::SerializeRedisCommand(argv, &cmd);
  send(sock, cmd.c_str(), cmd.size(), 0);

  char reply[1024];
  ssize_t reply_length = read(sock, reply, 1024);
  if (reply_length < 0) {
    std::cerr << "Read reply failed" << std::endl;
    close(sock);
    state->err = true;
    return;
  }
  std::string reply_str(reply, reply_length);

  close(sock);
  std::cout << "reply: " << reply_str << std::endl;
  if (reply_str.find("Replication") != std::string::npos) {
    state->err = false;
  } else {
    state->err = true;
  }
  parseInfoReplication(reply_str, state->replication);
  std::cout << "Role: " << state->replication.role << std::endl;
  std::cout << "Connected Slaves: " << state->replication.connected_slaves << std::endl;
  std::cout << "master_link_status: " << state->replication.master_link_status << std::endl;
  std::cout << "DB Binlog Filenum: " << state->replication.db_binlog_filenum << std::endl;
  std::cout << "DB Binlog Offset: " << state->replication.db_binlog_offset << std::endl;
  for (const auto& slave : state->replication.slaves) {
    std::cout << "Slave IP: " << slave.ip << std::endl;
    std::cout << "Slave Port: " << slave.port << std::endl;
    std::cout << "Slave Offset: " << slave.offset << std::endl;
  }
  states_.emplace_back(state);
}

// slaveof 命令
static bool Slaveof(const std::string& addr, const std::string& newMasterAddr) {
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
  argv.emplace_back("Slaveof");
  argv.emplace_back(master_ip);
  argv.emplace_back(std::to_string(master_port));
  net::SerializeRedisCommand(argv, &cmd);
  send(sock, cmd.c_str(), cmd.size(), 0);

  char reply[1025];
  ssize_t reply_length = read(sock, reply, 1024);
  std::string reply_str(reply, reply_length);
  close(sock);
  std::cout << "reply: " << reply_str << std::endl;
  bool success = false;
  success = reply_str.find("+OK") != std::string::npos;
  return success;
}

// slaveof no one 命令
static bool Slavenoone(const std::string& addr) {
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
  argv.emplace_back("Slaveof");
  argv.emplace_back("no");
  argv.emplace_back("one");
  net::SerializeRedisCommand(argv, &cmd);
  send(sock, cmd.c_str(), cmd.size(), 0);
  char reply[1024];
  ssize_t reply_length = read(sock, reply, 1024);
  std::string reply_str(reply, reply_length);

  close(sock);
  bool success = false;
  std::cout << "reply: " << reply_str << std::endl;
  success = reply_str.find("+OK") != std::string::npos;
  return success;
}

} // namespace pikiwidb