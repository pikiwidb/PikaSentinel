/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <algorithm>

#include "log.h"
#include "client.h"
#include "config.h"
#include "pikiwidb.h"
#include "pstd_string.h"

namespace pikiwidb {

    thread_local PClient *PClient::s_current = 0;

    std::mutex monitors_mutex;
    std::set<std::weak_ptr<PClient>, std::owner_less<std::weak_ptr<PClient> > > monitors;

    void CmdRes::SetRes(CmdRes::CmdRet _ret, const std::string &content) {
        ret_ = _ret;
        switch (ret_) {
            case kOK:
                SetLineString("+OK");
                break;
            case kPong:
                SetLineString("+PONG");
                break;
            case kSyntaxErr:
                SetLineString("-ERR syntax error");
                break;
            case kInvalidInt:
                SetLineString("-ERR value is not an integer or out of range");
                break;
            case kInvalidBitInt:
                SetLineString("-ERR bit is not an integer or out of range");
                break;
            case kInvalidBitOffsetInt:
                SetLineString("-ERR bit offset is not an integer or out of range");
                break;
            case kWrongBitOpNotNum:
                SetLineString("-ERR BITOP NOT must be called with a single source key.");
                break;
            case kInvalidBitPosArgument:
                SetLineString("-ERR The bit argument must be 1 or 0.");
                break;
            case kInvalidFloat:
                SetLineString("-ERR value is not a valid float");
                break;
            case kOverFlow:
                SetLineString("-ERR increment or decrement would overflow");
                break;
            case kNotFound:
                SetLineString("-ERR no such key");
                break;
            case kOutOfRange:
                SetLineString("-ERR index out of range");
                break;
            case kInvalidPwd:
                SetLineString("-ERR invalid password");
                break;
            case kNoneBgsave:
                SetLineString("-ERR No BGSave Works now");
                break;
            case kPurgeExist:
                SetLineString("-ERR binlog already in purging...");
                break;
            case kInvalidParameter:
                SetLineString("-ERR Invalid Argument");
                break;
            case kWrongNum:
                AppendStringRaw("-ERR wrong number of arguments for '");
                AppendStringRaw(content);
                AppendStringRaw("' command\r\n");
                break;
            case kInvalidIndex:
                AppendStringRaw("-ERR invalid DB index for '");
                AppendStringRaw(content);
                AppendStringRaw("'\r\n");
                break;
            case kInvalidDbType:
                AppendStringRaw("-ERR invalid DB for '");
                AppendStringRaw(content);
                AppendStringRaw("'\r\n");
                break;
            case kInconsistentHashTag:
                SetLineString("-ERR parameters hashtag is inconsistent");
            case kInvalidDB:
                AppendStringRaw("-ERR invalid DB for '");
                AppendStringRaw(content);
                AppendStringRaw("'\r\n");
                break;
            case kErrOther:
                AppendStringRaw("-ERR ");
                AppendStringRaw(content);
                AppendStringRaw(CRLF);
                break;
            case KIncrByOverFlow:
                AppendStringRaw("-ERR increment would produce NaN or Infinity");
                AppendStringRaw(content);
                AppendStringRaw(CRLF);
                break;
            case kInvalidCursor:
                AppendStringRaw("-ERR invalid cursor");
                break;
            case kWrongLeader:
                AppendStringRaw("-ERR wrong leader");
                AppendStringRaw(content);
                AppendStringRaw(CRLF);
            default:
                break;
        }
    }

    CmdRes::~CmdRes() { message_.clear(); }

    int PClient::handlePacket(pikiwidb::TcpConnection *obj, const char *start, int bytes) {
        s_current = this;

        const char *const end = start + bytes;
        const char *ptr = start;

        auto parseRet = parser_.ParseRequest(ptr, end);
        if (parseRet == PParseResult::error) {
            if (!parser_.IsInitialState()) {
                tcp_connection_->ActiveClose();
                return 0;
            }

            // try inline command
            /* std::vector<std::string> params;
             auto len = processInlineCmd(ptr, bytes, params);
             if (len == 0) {
               return 0;
             }

             ptr += len;
             parser_.SetParams(params);
             parseRet = PParseResult::ok;*/
        } else if (parseRet != PParseResult::ok) {
            return static_cast<int>(ptr - start);
        }

        DEFER { reset(); };

        // handle packet
        const auto &params = parser_.GetParams();
        if (params.empty()) {
            FeedMonitors(params_);
            return static_cast<int>(ptr - start);
        }
        argv_ = params;
        std::string cmd(params[0]);
        std::transform(params[0].begin(), params[0].end(), cmd.begin(), ::tolower);
        cmdName_ = cmd;
        pstd::StringToLower(cmdName_);

        /*if (!auth_) {
          if (cmd == "auth") {
            auto now = ::time(nullptr);
            if (now <= last_auth_ + 1) {
              // avoid guess password.
              tcp_connection_->ActiveClose();
              return 0;
            } else {
              last_auth_ = now;
            }
          } else {
            ReplyError(PError_needAuth, &reply_);
            return static_cast<int>(ptr - start);
          }
        }*/

        DEBUG("client {}, cmd {}", tcp_connection_->GetUniqueId(), cmd);

        FeedMonitors(params);

        // check readonly slave and execute command

        g_pikiwidb->SubmitFast(std::make_shared<CmdThreadPoolTask>(shared_from_this()));
        //if (cmd=="") handlePacketNew(obj, params, cmd);

        return static_cast<int>(ptr - start);
    }

    PClient *PClient::Current() { return s_current; }

    PClient::PClient(TcpConnection *obj) : tcp_connection_(obj), flag_(0), name_("clientxxx") {
        auth_ = false;
        reset();
    }

    int PClient::HandlePackets(pikiwidb::TcpConnection *obj, const char *start, int size) {
        int total = 0;

        while (total < size) {
            auto processed = handlePacket(obj, start + total, size - total);
            if (processed <= 0) {
                break;
            }

            total += processed;
        }

        //obj->SendPacket(reply_.ReadAddr(), reply_.ReadableSize());
        //reply_.Clear();
        return total;
    }

    void PClient::OnConnect() {
        SetState(ClientState::kOK);
        if (g_config.password.empty()) {
            SetAuth();
        }
    }

    void PClient::WriteReply2Client() {
        if (auto c = GetTcpConnection(); c) {
            c->SendPacket(Message());
        }
        Clear();
        reset();
    }

    void PClient::Close() {
        SetState(ClientState::kClosed);
        if (tcp_connection_) {
            tcp_connection_->ActiveClose();
        }
    }

    void PClient::reset() {
        s_current = nullptr;
        parser_.Reset();
    }

    void PClient::FeedMonitors(const std::vector<std::string> &params) {
        assert(!params.empty());

        {
            std::unique_lock<std::mutex> guard(monitors_mutex);
            if (monitors.empty()) {
                return;
            }
        }

        char buf[512];
        int n = snprintf(buf, sizeof buf, "+[%s:%d]: \"", s_current->tcp_connection_->GetPeerIp().c_str(),
                         s_current->tcp_connection_->GetPeerPort());

        assert(n > 0);

        for (const auto &e: params) {
            if (n < static_cast<int>(sizeof buf)) {
                n += snprintf(buf + n, sizeof buf - n, "%s ", e.data());
            } else {
                break;
            }
        }

        --n;  // no space follow last param

        {
            std::unique_lock<std::mutex> guard(monitors_mutex);

            for (auto it(monitors.begin()); it != monitors.end();) {
                auto m = it->lock();
                if (m) {
                    m->tcp_connection_->SendPacket(buf, n);
                    m->tcp_connection_->SendPacket("\"" CRLF, 3);

                    ++it;
                } else {
                    monitors.erase(it++);
                }
            }
        }
    }


}  // namespace pikiwidb
