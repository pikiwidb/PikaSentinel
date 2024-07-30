// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "redis_cli.h"
#include <cstdarg>
#include <cstring>
#include <atomic>
#include <string>
#include <utility>
#include <vector>
#include <assert.h>

namespace net {

/* Read a long long value starting at *s, under the assumption that it will be
 * terminated by \r\n. Ambiguously returns -1 for unexpected input. */
static long long readLongLong(char* s) {
  long long v = 0;
  int dec;
  int mult = 1;
  char c;

  if (*s == '-') {
    mult = -1;
    s++;
  } else if (*s == '+') {
    mult = 1;
    s++;
  }

  while ((c = *(s++)) != '\r') {
    dec = c - '0';
    if (dec >= 0 && dec < 10) {
      v *= 10;
      v += dec;
    } else {
      /* Should not happen... */
      return -1;
    }
  }

  return mult * v;
}



// Calculate the number of bytes needed to represent an integer as string.
static int intlen(int i) {
  int len = 0;
  if (i < 0) {
    len++;
    i = -i;
  }
  do {
    len++;
    i /= 10;
  } while (i != 0);
  return len;
}

// Helper that calculates the bulk length given a certain string length.
static size_t bulklen(size_t len) { return 1 + intlen(static_cast<int32_t>(len)) + 2 + len + 2; }

int redisvFormatCommand(std::string* cmd, const char* format, va_list ap) {
  const char* c = format;
  std::string curarg;
  char buf[1048576];
  std::vector<std::string> args;
  int touched = 0; /* was the current argument touched? */
  size_t totlen = 0;

  while (*c != '\0') {
    if (*c != '%' || c[1] == '\0') {
      if (*c == ' ') {
        if (touched != 0) {
          args.push_back(curarg);
          totlen += bulklen(curarg.size());
          curarg.clear();
          touched = 0;
        }
      } else {
        curarg.append(c, 1);
        touched = 1;
      }
    } else {
      char* arg = nullptr;
      size_t size = 0;

      switch (c[1]) {
        case 's':
          arg = va_arg(ap, char*);
          size = strlen(arg);
          if (size > 0) {
            curarg.append(arg, size);
          }
          break;
        case 'b':
          arg = va_arg(ap, char*);
          size = va_arg(ap, size_t);
          if (size > 0) {
              curarg.append(arg, size);
          }
          break;
        case '%':
          curarg.append(arg, size);
          break;
        default:
            /* Try to detect printf format */
        {
          static const char intfmts[] = "diouxX";
          char _format[16];
          const char* _p = c + 1;
          size_t _l = 0;
          va_list _cpy;
          bool fmt_valid = false;

          /* Flags */
          if (*_p != '\0' && *_p == '#') {
            _p++;
          }
          if (*_p != '\0' && *_p == '0') {
            _p++;
          }
          if (*_p != '\0' && *_p == '-') {
            _p++;
          }
          if (*_p != '\0' && *_p == ' ') {
            _p++;
          }
          if (*_p != '\0' && *_p == '+') {
            _p++;
          }

          /* Field width */
          while (*_p != '\0' && (isdigit(*_p) != 0)) {
            _p++;
          }

          /* Precision */
          if (*_p == '.') {
            _p++;
            while (*_p != '\0' && (isdigit(*_p) != 0)) {
              _p++;
            }
          }

          /* Copy va_list before consuming with va_arg */
          va_copy(_cpy, ap);

          if (strchr(intfmts, *_p)) {
            /* Integer conversion (without modifiers) */
            va_arg(ap, int);
            fmt_valid = true;
          } else if (strchr("eEfFgGaA", *_p)) {
              /* Double conversion (without modifiers) */
              va_arg(ap, double);
              fmt_valid = true;
          } else if (_p[0] == 'h' && _p[1] == 'h') { /* Size: char */
            _p += 2;
            if (*_p != '\0' && strchr(intfmts, *_p)) {
              va_arg(ap, int); /* char gets promoted to int */
              fmt_valid = true;
            }
          } else if (_p[0] == 'h') { /* Size: short */
            _p += 1;
            if (*_p != '\0' && strchr(intfmts, *_p)) {
              va_arg(ap, int); /* short gets promoted to int */
              fmt_valid = true;
            }
          } else if (_p[0] == 'l' && _p[1] == 'l') { /* Size: long long */
            _p += 2;
            if (*_p != '\0' && strchr(intfmts, *_p)) {
              va_arg(ap, long long);
              fmt_valid = true;
            }
          } else if (_p[0] == 'l') { /* Size: long */
            _p += 1;
            if (*_p != '\0' && strchr(intfmts, *_p)) {
              va_arg(ap, long);
              fmt_valid = true;
            }
          }

          if (!fmt_valid) {
            va_end(_cpy);
            return -1;
          }

          _l = (_p + 1) - c;
          if (_l < sizeof(_format) - 2) {
            memcpy(_format, c, _l);
            _format[_l] = '\0';

            int n = vsnprintf(buf, sizeof(buf), _format, _cpy);
            curarg.append(buf, n);

            /* Update current position (note: outer blocks
             * increment c twice so compensate here) */
            c = _p - 1;
          }

          va_end(_cpy);
          break;
        }
      }

      if (curarg.empty()) {
        return -1;
      }

      touched = 1;
      c++;
    }
    c++;
  }

  /* Add the last argument if needed */
  if (touched != 0) {
    args.push_back(curarg);
    totlen += bulklen(curarg.size());
  }

  /* Add bytes needed to hold multi bulk count */
  totlen += 1 + intlen(static_cast<int32_t>(args.size())) + 2;

  /* Build the command at protocol level */
  cmd->clear();
  cmd->reserve(totlen);

  cmd->append(1, '*');
  cmd->append(std::to_string(args.size()));
  cmd->append("\r\n");
  for (auto & arg : args) {
    cmd->append(1, '$');
    cmd->append(std::to_string(arg.size()));
    cmd->append("\r\n");
    cmd->append(arg);
    cmd->append("\r\n");
  }
  assert(cmd->size() == totlen);

  return static_cast<int32_t>(totlen);
}

int redisvAppendCommand(std::string* cmd, const char* format, va_list ap) {
  int len = redisvFormatCommand(cmd, format, ap);
  if (len == -1) {
    return -1;
  }

  return 0;
}

int redisFormatCommandArgv(RedisCmdArgsType argv, std::string* cmd) {
  size_t argc = argv.size();

  size_t totlen = 1 + intlen(static_cast<int32_t>(argc)) + 2;
  for (size_t i = 0; i < argc; i++) {
    totlen += bulklen(argv[i].size());
  }

  cmd->clear();
  cmd->reserve(totlen);

  cmd->append(1, '*');
  cmd->append(std::to_string(argc));
  cmd->append("\r\n");
  for (size_t i = 0; i < argc; i++) {
    cmd->append(1, '$');
    cmd->append(std::to_string(argv[i].size()));
    cmd->append("\r\n");
    cmd->append(argv[i]);
    cmd->append("\r\n");
  }

  return 0;
}

int SerializeRedisCommand(std::string* cmd, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  int result = redisvAppendCommand(cmd, format, ap);
  va_end(ap);
  return result;
}

int SerializeRedisCommand(RedisCmdArgsType argv, std::string* cmd) { return redisFormatCommandArgv(std::move(argv), cmd); }

};  // namespace net
