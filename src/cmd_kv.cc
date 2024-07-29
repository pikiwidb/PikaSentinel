/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd_kv.h"
#include "pikiwidb.h"
#include <iostream>
#include <aws/core/auth/AWSCredentials.h>

#include "aws/s3/S3Client.h"
#include "aws/core/Aws.h"
#include "aws/core/auth/AWSAuthSignerProvider.h"
#include "aws/S3/model/PutObjectRequest.h"
#include "nlohmann/json.hpp"

bool isValidJson(const std::string& str) {
  try {
    nlohmann::json::parse(str);
    return true;
  } catch (nlohmann::json::parse_error& e) {
    return false;
  }
}

namespace pikiwidb {
  bool uploadfile(const std::string &bucket_name, const std::string &bucket_path, const std::string &content) {
    Aws::SDKOptions m_options;
    Aws::S3::S3Client *m_client2 = {NULL};

    Aws::InitAPI(m_options);
    Aws::Client::ClientConfiguration cfg;
    cfg.endpointOverride = "http://beijing2.xstore.qihoo.net";  // S3服务器地址和端口
    cfg.scheme = Aws::Http::Scheme::HTTP;
    cfg.verifySSL = false;
    Aws::Auth::AWSCredentials cred("YHDIJ1LCITN7YHLETHLW", "fR5b2hEOzeogmiR01FzvYpb9BNt8eSrt0crHy510");  // 认证的Key

    auto m_client = Aws::S3::S3Client(cred, nullptr, cfg);
    //new S3Client(Aws::Auth::AWSCredentials("test", "12345678"), cfg, false, false);

    Aws::S3::Model::PutObjectRequest putObjectRequest;
    putObjectRequest.SetBucket(Aws::String(bucket_name));
    putObjectRequest.SetKey(Aws::String(bucket_path));
    //putObjectRequest.WithBucket(BucketName.c_str()).WithKey(objectKey.c_str());

    // auto inputData =
    //       Aws::MakeShared<Aws::FStream>(object_path.c_str(), local_file.c_str(),
    //                                   std::ios_base::in | std::ios_base::out);

    // Aws::MakeShared<Aws::FStream>(pathkey.c_str(), pathkey.c_str(), std::ios_base::in | std::ios_base::binary);
    auto input_data = Aws::MakeShared<Aws::StringStream>(content.c_str());
    //auto input_data = Aws::MakeShared<Aws::FStream>(pathkey.c_str(), pathkey.c_str(), std::ios_base::in | std::ios_base::binary);
    //Aws::MakeShared<Aws::FStream>("PutObjectInputStream", "/Users/charlieqiao/Desktop/bz.cc", std::ios_base::in | std::ios_base::binary);
    putObjectRequest.SetBody(input_data);
    auto putObjectResult = m_client.PutObject(putObjectRequest);
    if (putObjectResult.IsSuccess()) {
      std::cout << "Done!" << std::endl;
      return true;
    } else {
      std::cout << "PutObject error: " <<
                putObjectResult.GetError().GetExceptionName() << " " <<
                putObjectResult.GetError().GetMessage() << std::endl;
      return false;
    }
    /*    if (m_client != nullptr)
        {
            delete m_client;
            m_client = NULL;
        }*/
    Aws::ShutdownAPI(m_options);
  }


  UpLoadMetaCmd::UpLoadMetaCmd(const std::string &name, int16_t arity)
          : BaseCmd(name, arity, kCmdFlagsReadonly, kAclCategoryRead | kAclCategoryString) {}

  bool UpLoadMetaCmd::DoInitial(PClient *client) {
    //todo 参数判读
    return true;
  }


  void UpLoadMetaCmd::DoCmd(PClient *client) {
    // "{\n\t\"term_id\":\t-1,\n\t\"group_id\":\t-1,\n\t\"s3_bucket\":\t\"pulsar-s3-test-beijing2\",\n\t\
    // "s3_path\":\t\"/db/db0/0/CLOUDMANIFEST\",\n\t\"content\":\t\"sDRj9wIAAQEBE/7mvhIAAQIQNGM2Njc2ZmViMzRiMmExYQ==\"\n}"
    auto json = nlohmann::json::parse(client->argv_[1]);
    std::cout << "term id:" << json.at("term_id") << std::endl;
    std::cout << "group id: " << json.at("group_id") << std::endl;
    std::cout << "s3_bucket: " << json.at("s3_bucket") << std::endl;
    std::cout << "s3_path: " << json.at("s3_path") << std::endl;
    std::cout << "content: " << json.at("content") << std::endl;
    client->SetRes(CmdRes::kOK);
    client->CmdName();
    uploadfile(json.at("s3_bucket"), json.at("s3_path"), json.at("content"));
    // client->SetRes(CmdRes::kErrOther, s.ToString());
    //client->AppendStringRaw("");
    /*
    PString value;
    uint64_t ttl = -1;
    storage::Status s = PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->GetWithTTL(client->Key(), &value, &ttl);
    if (s.ok()) {
      client->AppendString(value);
    } else if (s.IsNotFound()) {
      client->AppendString("");
    } else {
      client->SetRes(CmdRes::kSyntaxErr, "get key error");
    }*/
  }

  UpdateGroupCmd::UpdateGroupCmd(const std::string &name, int16_t arity)
          : BaseCmd(name, arity, kCmdFlagsReadonly, kAclCategoryRead | kAclCategoryString) {}

  bool UpdateGroupCmd::DoInitial(PClient *client) {
    return true;
  }

  void UpdateGroupCmd::DoCmd(PClient *client) {
    if (isValidJson(client->argv_[1])) {
      auto jsonData = nlohmann::json::parse(client->argv_[1]);
      g_sentinel_service->UpdateGroup(jsonData);
      client->SetRes(CmdRes::kOK);
    } else {
      client->SetRes(CmdRes::kSyntaxErr);
    }
  }

  DelGroupCmd::DelGroupCmd(const std::string &name, int16_t arity)
          : BaseCmd(name, arity, kCmdFlagsReadonly, kAclCategoryRead | kAclCategoryString) {}

  bool DelGroupCmd::DoInitial(PClient *client) {
    return true;
  }

  void DelGroupCmd::DoCmd(PClient *client) {
    if (isValidJson(client->argv_[1])) {
      auto jsonData = nlohmann::json::parse(client->argv_[1]);
      int index = jsonData.at("index").get<int>();
      g_sentinel_service->DelGroup(index);
      client->SetRes(CmdRes::kOK);
    } else {
      client->SetRes(CmdRes::kSyntaxErr);
    }
  }

}  // namespace pikiwidb
