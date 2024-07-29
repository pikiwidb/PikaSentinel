/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd_table_manager.h"

#include <memory>


#include "cmd_kv.h"

namespace pikiwidb {

#define ADD_COMMAND(cmd, argc)                                                      \
  do {                                                                              \
    std::unique_ptr<BaseCmd> ptr = std::make_unique<cmd##Cmd>(kCmdName##cmd, argc); \
    cmds_->insert(std::make_pair(kCmdName##cmd, std::move(ptr)));                   \
  } while (0)

#define ADD_COMMAND_GROUP(cmd, argc)                                                \
  do {                                                                              \
    std::unique_ptr<BaseCmd> ptr = std::make_unique<Cmd##cmd>(kCmdName##cmd, argc); \
    cmds_->insert(std::make_pair(kCmdName##cmd, std::move(ptr)));                   \
  } while (0)

#define ADD_SUBCOMMAND(cmd, subcmd, argc)                                                                 \
  do {                                                                                                    \
    auto it##cmd = cmds_->find(kCmdName##cmd);                                                            \
    auto ptr##cmd = std::unique_ptr<BaseCmdGroup>(static_cast<BaseCmdGroup*>(it##cmd->second.release())); \
    ptr##cmd->AddSubCmd(std::make_unique<Cmd##cmd##subcmd>(kSubCmdName##cmd##subcmd, argc));              \
    it##cmd->second = std::move(ptr##cmd);                                                                \
  } while (0)

CmdTableManager::CmdTableManager() {
  cmds_ = std::make_unique<CmdTable>();
  cmds_->reserve(300);
}

void CmdTableManager::InitCmdTable() {
  std::unique_lock wl(mutex_);
  // kv
  ADD_COMMAND(UpLoadMeta, 2);
}

std::pair<BaseCmd*, CmdRes::CmdRet> CmdTableManager::GetCommand(const std::string& cmdName, PClient* client) {
  std::shared_lock rl(mutex_);

  auto cmd = cmds_->find(cmdName);

  if (cmd == cmds_->end()) {
    return std::pair(nullptr, CmdRes::kSyntaxErr);
  }

  /*if (cmd->second->HasSubCommand()) {
    if (client->argv_.size() < 2) {
      return std::pair(nullptr, CmdRes::kInvalidParameter);
    }
    return std::pair(cmd->second->GetSubCmd(client->argv_[1]), CmdRes::kSyntaxErr);
  }*/
  return std::pair(cmd->second.get(), CmdRes::kSyntaxErr);
}

bool CmdTableManager::CmdExist(const std::string& cmd) const {
  std::shared_lock rl(mutex_);
  return cmds_->find(cmd) != cmds_->end();
}

uint32_t CmdTableManager::GetCmdId() { return ++cmdId_; }
}  // namespace pikiwidb
