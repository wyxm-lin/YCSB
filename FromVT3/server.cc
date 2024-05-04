#include <sys/mman.h>

#include <fstream>
#include <iostream>
#include <map>
#include <queue>
#include <stack>
#include <string>
#include "common.h"

using namespace std;

int fd;
struct stat sb;
string db_name = "/home/linguangming/wyxm/JusteRPC/xihu/disk/db.txt"; // 这个在14号机
void *blk;
int session = -1;

static erpc::Nexus *nexus;
static erpc::MsgBuffer reqs[N];
static erpc::MsgBuffer resps[N];
static std::queue<int64_t> avail;
static erpc::Rpc<erpc::CTransport> *rpc;

void InitDB() {
    fd = open(db_name.c_str(), O_RDWR);
    if (fd == -1) {
        cerr << "Error opening file" << endl;
        exit(1);
    }
    cout << "file open OK\n";
    if (fstat(fd, &sb) == -1) {
        cerr << "Could not get file size" << endl;
        close(fd);
        exit(1);
    }
    blk = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (blk == MAP_FAILED) {
        cerr << "Error mapping the file with huge pages" << endl;
        close(fd);
        exit(1);
    }
    cout << "mmap OK\n";
    // memset(blk, 0, sb.st_size); // 没有必要
    
    char* check = (char*)blk;
    for (int i = 0; i < 10; i++) {
        cout << check[i];
    }
    cout << "\nCheck OK\n";
}

static void cont_func(void *, void *cbctx) {
  int64_t kk = (int64_t)cbctx;
  avail.push(kk);
}

static void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

void req_handler(erpc::ReqHandle *req_handle, void *) {
  // cout << "req_handler\n";
  // auto *msg = reinterpret_cast<CommonMsg *>(req_handle->get_req_msgbuf()->buf_);
  // auto &ctx = msg->u.Request;
  // uint64_t blockID = ctx.bid;
  // char *ptr = (char*)blk + blockID * BLOCKSIZE;
  // cout << "blockID: " << blockID << "\n";
  // if (msg->type == MsgType::Read) {
  //   cout << "Read\n";
  //   int64_t k = avail.front();
  //   avail.pop();
  //   erpc::MsgBuffer &req = reqs[k];
  //   erpc::MsgBuffer &resp = resps[k];
  //   CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
  //   msg->type = MsgType::CM;
  //   msg->u.CM.ms = ctx.ms;
  //   // better_memcpy(msg + 1, ptr, BLOCKSIZE);
  //   memcpy(msg + 1, (void*)ptr, BLOCKSIZE); // 完成实际拷贝
  //   cout << "memcpy OK\n";
  //   rpc->resize_msg_buffer(&req, sizeof(CommonMsg) + BLOCKSIZE);
  //   cout << "resize OK\n";
  //   rpc->enqueue_request(session, kReqType, &req, &resp, cont_func,
  //                          (void *)k);
  //   cout << "enqueue OK\n";
  // }
  // else if (msg->type == MsgType::Write) {
  //   cout << "Write\n";
  //   // better_memcpy(ptr, msg + 1, BLOCKSIZE);
  //   memcpy((void*)ptr, msg + 1, BLOCKSIZE); // 完成实际拷贝
  //   int64_t k = avail.front();
  //   avail.pop();
  //   erpc::MsgBuffer &req = reqs[k];
  //   erpc::MsgBuffer &resp = resps[k];
  //   CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
  //   msg->type = MsgType::CM;
  //   msg->u.CM.ms = ctx.ms;
  //   rpc->resize_msg_buffer(&req, sizeof(CommonMsg));
  //   rpc->enqueue_request(session, kReqType, &req, &resp, cont_func,
  //                          (void *)k);
  // }
  // else {
  //   exit(-1);
  // }
  // // 以下这个其实不重要
  // auto &resp = req_handle->pre_resp_msgbuf_;
  // rpc->enqueue_response(req_handle, &resp);

  cout << "get the request\n";
  auto *msg = reinterpret_cast<CommonMsg *>(req_handle->get_req_msgbuf()->buf_);
  auto &ctx = msg->u.Request;
  uint64_t blockID = ctx.bid;
  char *ptr = (char*)blk + blockID * BLOCKSIZE;
  cout << "blockID: " << blockID << "\n";

  if (msg->type == MsgType::Read) {
    cout << "read\n";
    auto &resp = req_handle->pre_resp_msgbuf_;
    memcpy(resp.buf_, ptr, BLOCKSIZE); // 完成读的copy
    rpc->enqueue_response(req_handle, &resp);
  }
  else if (msg->type == MsgType::Write) {
    cout << "write\n";
    memcpy((void*)ptr, msg + 1, BLOCKSIZE); // 完成实际拷贝
    auto &resp = req_handle->pre_resp_msgbuf_;
    rpc->enqueue_response(req_handle, &resp);
  }
  else {
    exit(-1);
  }
}

int main(int argc, char* argv[]) {
    // if (argc != 3) {
    //     cerr << "Usage: " << argv[0] << " <serverip> <clientip>" << endl;
    //     return 1;
    // }
    if (argc != 3) {
      cerr << "Usage: " << argv[0] << " <serverip> <clientip>" << endl;
      return 1;
    }

    string serverip = argv[1];
    InitDB();
    cout << "server storage OK\n";
    
    erpc::Nexus nexus(serverip.c_str());
    nexus.register_req_func(kReqType, req_handler);
    rpc = new erpc::Rpc<erpc::CTransport>(&nexus, nullptr, 0, sm_handler);
    for (int i = 0; i < N; i++) {
        avail.push(i);
        reqs[i] = rpc->alloc_msg_buffer_or_die(MAX_MSG_SIZE);
        resps[i] = rpc->alloc_msg_buffer_or_die(MAX_MSG_SIZE);
    }

    // rpc->run_event_loop(3000); // 用于client连接

    // cout << "connect to client\n";
    // session = rpc->create_session(argv[2], 0);
    // while (!rpc->is_connected(session)) rpc->run_event_loop_once();
	  // cout << "server connected to client" << endl;


    rpc->run_event_loop(10000000);
    delete rpc;
    return 0;
}