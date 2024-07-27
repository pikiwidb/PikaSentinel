/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "common.h"
#include "tcp_connection.h"

#include <set>
#include <span>
#include <unordered_map>
#include <unordered_set>

#include "proto_parser.h"

namespace pikiwidb {
    class CmdRes {
    public:
        enum CmdRet {
            kNone = 0,
            kOK,
            kPong,
            kSyntaxErr,
            kInvalidInt,
            kInvalidBitInt,
            kInvalidBitOffsetInt,
            kInvalidBitPosArgument,
            kWrongBitOpNotNum,
            kInvalidFloat,
            kOverFlow,
            kNotFound,
            kOutOfRange,
            kInvalidPwd,
            kNoneBgsave,
            kPurgeExist,
            kInvalidParameter,
            kWrongNum,
            kInvalidIndex,
            kInvalidDbType,
            kInvalidDB,
            kInconsistentHashTag,
            kErrOther,
            KIncrByOverFlow,
            kInvalidCursor,
            kWrongLeader,
        };

        CmdRes() = default;

        virtual ~CmdRes();

        bool None() const { return ret_ == kNone && message_.empty(); }

        bool Ok() const { return ret_ == kOK || ret_ == kNone; }

        void Clear() {
            message_.clear();
            ret_ = kNone;
        }

        inline const std::string &Message() const { return message_; };

        inline void AppendStringRaw(const std::string &value) { message_.append(value); }

        inline void SetLineString(const std::string &value) { message_ = value + CRLF; }

        void AppendString(const std::string &value);

        void AppendStringVector(const std::vector<std::string> &strArray);

        void RedisAppendLenUint64(std::string &str, uint64_t ori, const std::string &prefix) {
            RedisAppendLen(str, static_cast<int64_t>(ori), prefix);
        }

        void SetRes(CmdRet _ret, const std::string &content = "");

        inline void RedisAppendContent(std::string &str, const std::string &value) {
            str.append(value.data(), value.size());
            str.append(CRLF);
        }

        void RedisAppendLen(std::string &str, int64_t ori, const std::string &prefix);

    private:
        std::string message_;
        CmdRet ret_ = kNone;
    };

    enum ClientFlag {
        ClientFlag_multi = 0x1,
        ClientFlag_dirty = 0x1 << 1,
        ClientFlag_wrongExec = 0x1 << 2,
        ClientFlag_master = 0x1 << 3,
    };

    enum class ClientState {
        kOK,
        kClosed,
    };

    class PClient : public std::enable_shared_from_this<PClient>, public CmdRes {
    public:
        PClient() = delete;

        explicit PClient(TcpConnection *obj);

        int HandlePackets(pikiwidb::TcpConnection *, const char *, int);

        void OnConnect();

        EventLoop *GetEventLoop(void) const { return tcp_connection_->GetEventLoop(); }

        TcpConnection *GetTcpConnection(void) const { return tcp_connection_; }

        void Close();

        static PClient *Current();

        // multi
        void SetFlag(unsigned flag) { flag_ |= flag; }

        void ClearFlag(unsigned flag) { flag_ &= ~flag; }

        bool IsFlagOn(unsigned flag) { return flag_ & flag; }

        void FlagExecWrong() {
            if (IsFlagOn(ClientFlag_multi)) {
                SetFlag(ClientFlag_wrongExec);
            }
        }

        static void FeedMonitors(const std::vector<std::string> &params);

        void SetAuth() { auth_ = true; }

        bool GetAuth() const { return auth_; }

        void RewriteCmd(std::vector<std::string> &params) { parser_.SetParams(params); }

        void SetCmdName(const std::string &name) { cmdName_ = name; }

        const std::string &CmdName() const { return cmdName_; }

        inline size_t ParamsSize() const { return params_.size(); }

        inline ClientState State() const { return state_; }

        inline void SetState(ClientState state) { state_ = state; }

        void WriteReply2Client();

        // All parameters of this command (including the command itself)
        // e.g：["set","key","value"]
        std::vector<std::string> argv_;

    private:
        int handlePacket(pikiwidb::TcpConnection *, const char *, int);
        void reset();
        TcpConnection *const tcp_connection_;

        PProtoParser parser_;
        UnboundedBuffer reply_;

        std::unordered_set<std::string> channels_;
        std::unordered_set<std::string> pattern_channels_;

        unsigned flag_;
        std::unordered_map<int, std::unordered_set<std::string> > watch_keys_;
        std::vector<std::vector<std::string> > queue_cmds_;

        // blocked list
        std::unordered_set<std::string> waiting_keys_;
        std::string target_;

        std::string name_;
        // suchAs config
        std::string cmdName_;     // suchAs config

        // auth
        bool auth_ = false;
        time_t last_auth_ = 0;

        static thread_local PClient *s_current;
        // All parameters of this command (including the command itself)
        // e.g：["set","key","value"]
        std::vector<std::string> params_;
        ClientState state_;
    };

}  // namespace pikiwidb
