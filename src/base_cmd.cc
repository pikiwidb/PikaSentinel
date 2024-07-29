/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "base_cmd.h"
#include "common.h"
#include "config.h"
#include "log.h"
#include "pikiwidb.h"

namespace pikiwidb {

    BaseCmd::BaseCmd(std::string name, int16_t arity, uint32_t flag, uint32_t aclCategory) {
        name_ = std::move(name);
        arity_ = arity;
        flag_ = flag;
        acl_category_ = aclCategory;
    }

    bool BaseCmd::CheckArg(size_t num) const {
        if (arity_ > 0) {
            return num == arity_;
        }
        return num >= -arity_;
    }

    void BaseCmd::Execute(PClient *client) {
        if (!DoInitial(client)) {
            return;
        }
        DoCmd(client);
    }

    std::string BaseCmd::Name() const { return name_; }

    BaseCmdGroup::BaseCmdGroup(const std::string &name, uint32_t flag) : BaseCmdGroup(name, -2, flag) {}

    BaseCmdGroup::BaseCmdGroup(const std::string &name, int16_t arity, uint32_t flag) : BaseCmd(name, arity, flag, 0) {}

    void BaseCmdGroup::AddSubCmd(std::unique_ptr<BaseCmd> cmd) { subCmds_[cmd->Name()] = std::move(cmd); }

    bool BaseCmdGroup::DoInitial(PClient *client) {
        return true;
    }

}  // namespace pikiwidb
