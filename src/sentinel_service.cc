#include "sentinel_service.h"
#include "client.h"
#include <chrono>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <regex>
#include <map>
#include <sstream>
#include <json/json.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

struct InfoSlave {
    std::string ip;
    std::string port;
    std::string state;
    std::string offset;
};

struct InfoReplication {
    std::map<std::string, std::string> info;
    std::vector<InfoSlave> slaves;
};

InfoReplication parseInfoReplication(const std::string& text) {
    InfoReplication infoReplication;
    std::map<std::string, std::string> info;
    std::vector<std::map<std::string, std::string>> slaveMap;
    std::vector<InfoSlave> slaves;

    std::regex slavePattern("slave[0-9]+");
    std::istringstream stream(text);
    std::string line;

    while (std::getline(stream, line)) {
        std::size_t pos = line.find(':');
        if (pos == std::string::npos) {
            continue;
        }
        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        key = std::regex_replace(key, std::regex("^\\s+|\\s+$"), "");

        if (std::regex_match(key, slavePattern)) {
            std::istringstream kvStream(value);
            std::string kvPair;
            std::map<std::string, std::string> slave;
            while (std::getline(kvStream, kvPair, ',')) {
                std::size_t kvPos = kvPair.find('=');
                if (kvPos == std::string::npos) {
                    std::cerr << "Invalid replication info, slaveKvs = " << value << ", slaveKv = " << kvPair << std::endl;
                    continue;
                }
                std::string slaveKey = kvPair.substr(0, kvPos);
                std::string slaveValue = kvPair.substr(kvPos + 1);
                slave[slaveKey] = slaveValue;
            }
            slaveMap.push_back(slave);
        } else if (key.find("db0") == 0) {
            std::istringstream kvStream(value);
            std::string kvPair;
            while (std::getline(kvStream, kvPair, ',')) {
                std::size_t kvPos = kvPair.find('=');
                if (kvPos == std::string::npos) {
                    continue;
                }
                std::string dbKey = kvPair.substr(0, kvPos);
                std::string dbValue = kvPair.substr(kvPos + 1);
                if (dbKey == "binlog_offset") {
                    std::size_t offsetPos = dbValue.find(' ');
                    if (offsetPos != std::string::npos) {
                        info["binlog_file_num"] = dbValue.substr(0, offsetPos);
                        info["binlog_offset"] = dbValue.substr(offsetPos + 1);
                    }
                }
            }
        } else {
            info[key] = value;
        }
    }

    if (!slaveMap.empty()) {
        Json::Value slavesJson;
        for (const auto& slave : slaveMap) {
            Json::Value slaveJson;
            for (const auto& [k, v] : slave) {
                slaveJson[k] = v;
            }
            slavesJson.append(slaveJson);
        }
        Json::StreamWriterBuilder writer;
        std::string slavesStr = Json::writeString(writer, slavesJson);

        Json::CharReaderBuilder reader;
        Json::Value slavesRoot;
        std::string errs;
        std::istringstream s(slavesStr);
        if (!Json::parseFromStream(reader, s, &slavesRoot, &errs)) {
            std::cerr << "Unmarshal to slaves failed, " << errs << std::endl;
        } else {
            for (const auto& slave : slavesRoot) {
                InfoSlave infoSlave;
                infoSlave.ip = slave["ip"].asString();
                infoSlave.port = slave["port"].asString();
                infoSlave.state = slave["state"].asString();
                infoSlave.offset = slave["offset"].asString();
                slaves.push_back(infoSlave);
            }
        }
    }

    Json::Value infoJson;
    for (const auto& [k, v] : info) {
        infoJson[k] = v;
    }

    Json::StreamWriterBuilder writer;
    std::string infoStr = Json::writeString(writer, infoJson);

    Json::CharReaderBuilder reader;
    Json::Value infoRoot;
    std::string errs;
    std::istringstream s(infoStr);
    if (!Json::parseFromStream(reader, s, &infoRoot, &errs)) {
        std::cerr << "Unmarshal to infoReplication failed, " << errs << std::endl;
    } else {
        for (const auto& member : infoRoot.getMemberNames()) {
            infoReplication.info[member] = infoRoot[member].asString();
        }
        infoReplication.slaves = slaves;
    }

    return infoReplication;
}

namespace pikiwidb {

    PKPingService::PKPingService() : running_(false), client_(nullptr) {}

    PKPingService::~PKPingService() {
        Stop();
    }

    void PKPingService::Start() {
        running_ = true;
        thread_ = std::thread(&PKPingService::Run, this);
    }

    void PKPingService::Stop() {
        running_ = false;
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    void PKPingService::AddHost(const std::string& host, int port, int group_id, int term_id) {
        hosts_.emplace_back(host, port, group_id, term_id);
    }

    void PKPingService::Run() {
        while (running_) {
            for (const auto& [host, port, group_id, term_id] : hosts_) {
                bool result = PKPingRedis(host, port, group_id, term_id);
                if (client_) {
                    client_->GetTcpConnection()->SendPacket(msg.data(), msg.size());

                    // Call parseInfoReplication after PKPing and send the parsed result
                    std::string pkpingResult;
                    InfoReplication infoReplication = parseInfoReplication(pkpingResult);

                    // Convert infoReplication to a JSON string
                    Json::StreamWriterBuilder writer;
                    Json::Value root;
                    for (const auto& [key, value] : infoReplication.info) {
                        root[key] = value;
                    }
                    for (const auto& slave : infoReplication.slaves) {
                        Json::Value slaveJson;
                        slaveJson["ip"] = slave.ip;
                        slaveJson["port"] = slave.port;
                        slaveJson["state"] = slave.state;
                        slaveJson["offset"] = slave.offset;
                        root["slaves"].append(slaveJson);
                    }
                    std::string infoJsonStr = Json::writeString(writer, root);

                    // Send the parsed infoReplication to the client
                    client_->GetTcpConnection()->SendPacket(infoJsonStr.data(), infoJsonStr.size());
                }
                std::this_thread::sleep_for(std::chrono::seconds(1)); // Sleep for 1 second between pings
            }
            std::this_thread::sleep_for(std::chrono::seconds(10)); // Sleep for 10 seconds between each full cycle
        }
    }

    bool PKPingService::PKPingRedis(const std::string& host, int port, int group_id, int term_id) {
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
        pkping_cmd += "$" + std::to_string(std::to_string(term_id).length()) + "\r\n" + std::to_string(term_id) + "\r\n";

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

    struct UploadRequest {
        int group_id;
        int term_id;
        std::string s3_bucket;
        std::string s3_path;
        std::string content;
    };

    class ApiServer {
    public:
        void handle_upload_manifest_to_s3(http_request request);

    private:
        void upload_manifest_to_s3(int group_id, int term_id, const std::string& s3_bucket,
                                   const std::string& s3_path, const std::string& content);
        json::value api_response_error(const std::string& message);
        json::value api_response_json(const std::string& message);
    };

    void ApiServer::handle_upload_manifest_to_s3(http_request request) {
        request.extract_string().then([this](std::string body) {
            UploadRequest uploadReq;
            std::istringstream ss(body);
            json::value jsonValue;
            ss >> jsonValue;

            try {
                uploadReq.group_id = jsonValue[U("group_id")].as_integer();
                uploadReq.term_id = jsonValue[U("term_id")].as_integer();
                uploadReq.s3_bucket = jsonValue[U("s3_bucket")].as_string();
                uploadReq.s3_path = jsonValue[U("s3_path")].as_string();
                uploadReq.content = jsonValue[U("content")].as_string();

                upload_manifest_to_s3(uploadReq.group_id, uploadReq.term_id, uploadReq.s3_bucket,
                                      uploadReq.s3_path, uploadReq.content);
                request.reply(status_codes::OK, api_response_json("OK"));
            } catch (const std::exception& e) {
                request.reply(status_codes::BadRequest, api_response_error(e.what()));
            }
        }).wait();
    }

    void ApiServer::UploadManifestToS3(int gid, int tid, const std::string& bucket, const std::string& filename, const std::string& manifest)  {
        // Implement the actual upload logic here
        std::lock_guard<std::mutex> lock(mu);
        Context ctx = newContext();

        if (gid <= 0 || gid > MaxGroupId) {
            return "Invalid group id = " + std::to_string(gid) + ", out of range";
        }

        auto groupIt = ctx.group.find(gid);
        if (groupIt != ctx.group.end()) {
            if (groupIt->second.TermId == tid) {
                Aws::SDKOptions options;
                Aws::InitAPI(options);
                {
                    Aws::Client::ClientConfiguration clientConfig;
                    clientConfig.region = ConfigCloudSrcBucketRegion();
                    clientConfig.endpointOverride = ConfigCloudEndPointOverride();
                    clientConfig.scheme = Aws::Http::Scheme::HTTP;

                    Aws::Auth::AWSCredentials credentials(ConfigCloudAccessKey(), ConfigCloudSecretKey());
                    Aws::S3::S3Client s3_client(credentials, clientConfig);

                    std::ifstream file("/Users/charlieqiao/Desktop/bz.cc", std::ios::binary);
                    if (!file.is_open()) {
                        return "Unable to open file";
                    }

                    Aws::S3::Model::PutObjectRequest object_request;
                    object_request.WithBucket(bucket.c_str()).WithKey(filename.c_str());

                    auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", "/Users/charlieqiao/Desktop/bz.cc", std::ios_base::in | std::ios_base::binary);
                    object_request.SetBody(input_data);

                    auto put_object_outcome = s3_client.PutObject(object_request);
                    if (!put_object_outcome.IsSuccess()) {
                        return "Unable to upload [" + filename + "] to [" + bucket + "], [" + put_object_outcome.GetError().GetMessage() + "]";
                    }
                }
                Aws::ShutdownAPI(options);
            } else {
                return "Group-[" + std::to_string(gid) + "] term id:[" + std::to_string(tid) + "] not equal to pika term id:[" + std::to_string(groupIt->second.TermId) + "]";
            }
        } else {
            return "Group-[" + std::to_string(gid) + "] not exists";
        }
        return "Upload manifest success";
    }

    json::value ApiServer::api_response_error(const std::string& message) {
        json::value response;
        response[U("error")] = json::value::string(message);
        return response;
    }

    json::value ApiServer::api_response_json(const std::string& message) {
        json::value response;
        response[U("message")] = json::value::string(message);
        return response;
    }

    class Topom {
    public:
        std::mutex mu;

        // Add your constructor and other necessary member functions here

        Aws::String ConfigCloudAccessKey() const {
            // Return your cloud access key
        }

        Aws::String ConfigCloudSecretKey() const {
            // Return your cloud secret key
        }

        Aws::String ConfigCloudEndPointOverride() const {
            // Return your cloud endpoint override
        }

        Aws::String ConfigCloudSrcBucketRegion() const {
            // Return your cloud bucket region
        }

        struct Context {
            std::map<int, Group> group;
        };

        struct Group {
            int TermId;
        };

        Context newContext() {
            // Implement your context initialization
        }

    private:
        static const int MaxGroupId = 100; // Define your max group id
    };

}  // namespace pikiwidb