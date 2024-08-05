/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd.h"
#include "pikiwidb.h"
#include <iostream>
#include <aws/core/auth/AWSCredentials.h>
#include "aws/s3/S3Client.h"
#include "aws/core/Aws.h"
#include "aws/core/auth/AWSAuthSignerProvider.h"
#include "aws/s3/model/PutObjectRequest.h"
#include "nlohmann/json.hpp"
#include "config.h"
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>

bool isValidJson(const std::string& str) {
  try {
    nlohmann::json::parse(str);
    return true;
  } catch (nlohmann::json::parse_error& e) {
    return false;
  }
}

namespace pikiwidb {
  int base64_decode(std::string base64_str, char **output,  int *out_len) {
    BIO *bio = NULL;
    BIO *b64 = NULL;
    char *buffer = NULL;
    int buf_len = 0;
    int decoded_len = 0;
    int ret = 0;

    if (NULL == output || NULL == out_len) {
      return -1;
    }

    b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

    bio = BIO_new_mem_buf(base64_str.c_str(), base64_str.length());
    bio = BIO_push(b64, bio);

    buf_len = base64_str.length() * 3 / 4;
    buffer = (char *)calloc(1, buf_len + 1);
    if (NULL == buffer) {
      ret = -1;
      goto __Failed;
    }

    decoded_len = BIO_read(b64, buffer, base64_str.length());
    if (0 >= decoded_len) {
      ret = -1;
      goto __Failed;
    }
    buffer[decoded_len] = '\0';
    *output = (char *)buffer;
    *out_len = decoded_len;

    BIO_free_all(bio);
    return ret;

    __Failed:
    BIO_free_all(bio);
    free(buffer);
    return -1;
  }

  UpLoadMetaCmd::UpLoadMetaCmd(const std::string &name, int16_t arity)
          : BaseCmd(name, arity, kCmdFlagsReadonly) {}

  bool UpLoadMetaCmd::DoInitial(PClient *client) {
    return true;
  }

  void UpLoadMetaCmd::DoCmd(PClient *client) {
    if (client->argv_.size() != 2) {
      client->SetRes(CmdRes::kErrOther, "Err argv num");
      return;
    }
    if (!isValidJson(client->argv_[1])) {
      client->SetRes(CmdRes::kSyntaxErr);
      return;
    }
    auto json = nlohmann::json::parse(client->argv_[1]);
    if (json.contains("term_id") && json.contains("group_id")
        && json.contains("s3_bucket") && json.contains("s3_path")
        && json.contains("content")) {
        int group_id = json["group_id"];
        int term_id = json["term_id"];
        auto group = g_sentinel_service->GetGroup(group_id);
        if (group == nullptr) {
          client->SetRes(CmdRes::kErrOther, "group is not found");
          return;
        }
        if (group->term_id != term_id) {
          client->SetRes(CmdRes::kErrOther, "Term-ids are not equal");
          return;
        }

        Aws::SDKOptions m_options;
        Aws::InitAPI(m_options);
        Aws::Client::ClientConfiguration cfg;

        cfg.scheme = Aws::Http::Scheme::HTTP;
        cfg.verifySSL = false;
        cfg.endpointOverride =  pikiwidb::g_config.s3EndpointOverride;
        Aws::Auth::AWSCredentials cred(pikiwidb::g_config.s3AccessKey, pikiwidb::g_config.s3SecretKey);
        char *output = NULL;
        int out_len = 0;
        base64_decode(json.at("content"), &output, &out_len) ;

        auto m_client = Aws::S3::S3Client(cred, nullptr, cfg);
        Aws::S3::Model::PutObjectRequest putObjectRequest;
        putObjectRequest.SetBucket(Aws::String(json.at("s3_bucket")));
        putObjectRequest.SetKey(Aws::String(json.at("s3_path")));

        const std::shared_ptr<Aws::IOStream> inputData =
                Aws::MakeShared<Aws::StringStream>("");
        inputData->write(output, out_len);
        putObjectRequest.SetBody(inputData);

        auto putObjectResult = m_client.PutObject(putObjectRequest);
        if (putObjectResult.IsSuccess()) {
            client->SetRes(CmdRes::kOK);
            Aws::ShutdownAPI(m_options);
            return;
        } else {
            client->SetRes(CmdRes::kErrOther, putObjectResult.GetError().GetExceptionName() + " "
            + putObjectResult.GetError().GetMessage());
            Aws::ShutdownAPI(m_options);
            return;
        }
    }
    client->SetRes(CmdRes::kErrOther, "Err json");
  }

  UpdateGroupCmd::UpdateGroupCmd(const std::string &name, int16_t arity)
          : BaseCmd(name, arity, kCmdFlagsReadonly) {}

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
          : BaseCmd(name, arity, kCmdFlagsReadonly) {}

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
