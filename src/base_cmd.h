/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.h"

namespace pikiwidb {

// command definition
// base cmd
const std::string kCmdNameUpLoadMeta = "uploadmeta";
const std::string kCmdNameUpdateGroup = "updategroup";
const std::string kCmdNameDelGroup = "delgroup";
// admin
const std::string kCmdNameConfig = "config";
const std::string kSubCmdNameConfigGet = "get";
const std::string kSubCmdNameConfigSet = "set";


enum CmdFlags {
  kCmdFlagsWrite = (1 << 0),             // May modify the dataset
  kCmdFlagsReadonly = (1 << 1),          // Doesn't modify the dataset
  kCmdFlagsModule = (1 << 2),            // Implemented by a module
  kCmdFlagsAdmin = (1 << 3),             // Administrative command
  kCmdFlagsPubsub = (1 << 4),            // Pub/Sub related command
  kCmdFlagsNoscript = (1 << 5),          // Not allowed in Lua scripts
  kCmdFlagsBlocking = (1 << 6),          // May block the server
  kCmdFlagsSkipMonitor = (1 << 7),       // Don't propagate to MONITOR
  kCmdFlagsSkipSlowlog = (1 << 8),       // Don't log to slowlog
  kCmdFlagsFast = (1 << 9),              // Tagged as fast by developer
  kCmdFlagsNoAuth = (1 << 10),           // Skip ACL checks
  kCmdFlagsMayReplicate = (1 << 11),     // May replicate even if writes are disabled
  kCmdFlagsProtected = (1 << 12),        // Don't accept in scripts
  kCmdFlagsModuleNoCluster = (1 << 13),  // No cluster mode support
  kCmdFlagsNoMulti = (1 << 14),          // Cannot be pipelined
  kCmdFlagsExclusive = (1 << 15),        // May change Storage pointer, like pika's kCmdFlagsSuspend
  kCmdFlagsRaft = (1 << 16),             // raft
};

/**
 * @brief Base class for all commands
 * BaseCmd, as the base class for all commands, mainly implements some common functions
 * such as command name, number of parameters, command flag
 * All data related to a single command execution cannot be defined in Base Cmd and its derived classes.
 * Because the command may be executed in multiple threads at the same time, the data defined in the command
 * will be overwritten by other threads, causing the command to be executed incorrectly.
 * Therefore, the data related to the execution of the command must be defined in the `CmdContext` class.
 * The `CmdContext` class is passed to the command for execution.
 * Base Cmd and its derived classes only provide corresponding functions and logical processing for command execution,
 * but do not provide data storage.
 *
 * This avoids creating a new object every time a command is executed and reduces memory allocation
 * But some data that does not change during command execution
 * (data that does not need to be changed after command initialization) can be placed in Base Cmd
 * For example: command name, number of parameters, command flag, etc.
 */
class BaseCmd : public std::enable_shared_from_this<BaseCmd> {
 public:
  /**
   * @brief Construct a new Base Cmd object
   * @param name command name
   * @param arity number of parameters
   * @param flag command flag
   * @param aclCategory command acl category
   */
  BaseCmd(std::string name, int16_t arity, uint32_t flag);
  virtual ~BaseCmd() = default;

  // check that each parameter meets the requirements
  bool CheckArg(size_t num) const;

  // get the key in the current command
  // e.g: set myKey value, return myKey
  std::vector<std::string> CurrentKey(PClient* client) const;

  // the entry point for the entire cmd execution
  void Execute(PClient* client);

  std::string Name() const;

 protected:
  // Execute a specific command
  virtual void DoCmd(PClient* client) = 0;

  std::string name_;
  int16_t arity_ = 0;
  uint32_t flag_ = 0;

 private:
  // The function to be executed first before executing `DoCmd`
  // What needs to be done at present are: extract the key in the command and fill it into the context
  // If this function returns false, then Do Cmd will not be executed
  virtual bool DoInitial(PClient* client) = 0;

  //  virtual void Clear(){};
  //  BaseCmd& operator=(const BaseCmd&);
};

class BaseCmdGroup : public BaseCmd {
 public:
  BaseCmdGroup(const std::string& name, uint32_t flag);
  BaseCmdGroup(const std::string& name, int16_t arity, uint32_t flag);

  ~BaseCmdGroup() override = default;

  void AddSubCmd(std::unique_ptr<BaseCmd> cmd);

  // group cmd this function will not be called
  void DoCmd(PClient* client) override{};

  // group cmd this function will not be called
  bool DoInitial(PClient* client) override;

 private:
  std::map<std::string, std::unique_ptr<BaseCmd>> subCmds_;
};
}  // namespace pikiwidb
