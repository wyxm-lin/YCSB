#include <stdio.h>
#include <iostream>
#include "rpc.h"
#include <cstdarg>

#define debug

using std::string;

#define MSG_BUFFER_NUM 50
#define MAX_PKT_SIZE 3824
#define MAX_MSG_SIZE 8353520
#define SERVER_SIZE 0xA00000000    // 40GB
// #define BackgroundThreadNum 1

static constexpr uint8_t kReqType = 2;

enum RequestType { kRead = 0, kWrite, kEmpty };
typedef struct Request {
	RequestType req_type; // 请求类型
  	uint64_t blockID; // 块号
	uint64_t ops_id; // 操作号
} Request;

// 通用信息传递结构体
struct CommonMsg {
	RequestType req_type; // 请求类型
	uint64_t blockID; // 块号
	uint64_t ops_id; // 操作号
};

typedef struct Callback {
	int use_shared_buffer_id;
	int BeginBlockID;
} Callback;

#define BLOCKSIZE (3824 - sizeof(CommonMsg))
// #define BLOCKSIZE 4096

void mylog(const char* format, ...) {
#ifdef debug
	// va_list args;
	// va_start(args, format);
	// vprintf(format, args);
	// va_end(args);
#endif
}

/*

sudo wipefs -a /dev/nvme1n1
sudo mkfs.xfs /dev/nvme1n1


*/