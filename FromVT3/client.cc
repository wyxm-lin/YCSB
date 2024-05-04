#include <sys/mman.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <queue>
#include <stack>
#include <string>
#include <chrono>
#include "/usr/include/x86_64-linux-gnu/sys/time.h"
#include "common.h"
#include <string>

using namespace std;

uint32_t session;
static erpc::Nexus *nexus;
static erpc::MsgBuffer reqs[N];
static erpc::MsgBuffer resps[N];
static std::queue<int64_t> avail;
static erpc::Rpc<erpc::CTransport> *rpc;
char ReadBuffer[BLOCKSIZE]; // 用来中继而已
queue<Request> Requests;
int RequestTotNumber;
int OneRequestContainBlockSize;

int dbs_read(uint64_t, void *, uint64_t);
int dbs_write(uint64_t, void *, uint64_t);

static void cont_func(void *, void *ctx) {
  cout << "cont_func" << endl;
  int kk = (int64_t)ctx;
  avail.push(kk);

  auto *msg = reinterpret_cast<CommonMsg *>(reqs[kk].buf_);
  // if (msg->type != MsgType::CM) exit(-1);
  cout << "no error" << endl;
  auto ms = (Mission *)msg->u.Request.ms;
  cout << "no error" << endl;
  if (ms == nullptr) {
    cout << "ms is nullptr" << endl;
  }
  cout << ms->nxt << endl;
  cout << "no error" << endl;
  uint32_t seq = ms->nxt++;
  cout << "no error" << endl;
  if (ms->type == MsgType::Write) {
    if (seq >= ms->tot) {
      // auto &respbuf = req_handle->pre_resp_msgbuf_;
      // rpc->enqueue_response(req_handle, &respbuf);
      return;
    }

    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *sdmsg = reinterpret_cast<CommonMsg *>(req.buf_);
    sdmsg->type = MsgType::Write;
    auto &ctx = sdmsg->u.Request;
    ctx.bid = ms->bid + seq;
    ctx.ms = (uint64_t)ms;
    void *ptr = (void *)(ms->ptr + seq * BLOCKSIZE);
    // better_memcpy(sdmsg + 1, ptr, BLOCKSIZE);
    memcpy(sdmsg + 1, ReadBuffer, BLOCKSIZE); // 每次都是拿这个buffer来进行copy
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg) + BLOCKSIZE);
    rpc->enqueue_request(session, kReqType, &req, &resp,
                         cont_func, (void *)k);
  }
  else if (ms->type == MsgType::Read) {
    // 处理数据
    // void *dst = (void *)(ms->ptr + (seq - ms->pip) * BLOCKSIZE);
    // better_memcpy(dst, msg + 1, BLOCKSIZE);
    cout << "deal with data" << endl;
    memcpy(ReadBuffer, (void*)resps[kk].buf_, BLOCKSIZE); // 每次都是拿这个buffer来进行copy
    cout << "deal with data" << endl;
    if (seq >= ms->tot) {
      cout << "hereher\n";
      // auto &respbuf = req_handle->pre_resp_msgbuf_;
      // rpc->enqueue_response(req_handle, &respbuf);
      return;
    }
    cout << "here\n";
    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *sdmsg = reinterpret_cast<CommonMsg *>(req.buf_);
    sdmsg->type = MsgType::Read;
    auto &ctx = sdmsg->u.Request;
    ctx.bid = ms->bid + seq;
    ctx.ms = (uint64_t)ms;
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg));
    rpc->enqueue_request(session, kReqType, &req, &resp,
                         cont_func, (void *)k);
  }

  // auto &respbuf = req_handle->pre_resp_msgbuf_;
  // rpc->enqueue_response(req_handle, &respbuf);
}

static void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

void req_handler(erpc::ReqHandle *req_handle, void *) {
  auto *msg = reinterpret_cast<CommonMsg *>(req_handle->get_req_msgbuf()->buf_);
  if (msg->type != MsgType::CM) exit(-1);

  auto ms = (Mission *)msg->u.CM.ms;
  uint32_t seq = ms->nxt++;
  if (ms->type == MsgType::Write) {
    if (seq >= ms->tot) {
      auto &respbuf = req_handle->pre_resp_msgbuf_;
      rpc->enqueue_response(req_handle, &respbuf);
      return;
    }

    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *sdmsg = reinterpret_cast<CommonMsg *>(req.buf_);
    sdmsg->type = MsgType::Write;
    auto &ctx = sdmsg->u.Request;
    ctx.bid = ms->bid + seq;
    ctx.ms = (uint64_t)ms;
    void *ptr = (void *)(ms->ptr + seq * BLOCKSIZE);
    // better_memcpy(sdmsg + 1, ptr, BLOCKSIZE);
    memcpy(sdmsg + 1, ReadBuffer, BLOCKSIZE); // 每次都是拿这个buffer来进行copy
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg) + BLOCKSIZE);
    rpc->enqueue_request(session, kReqType, &req, &resp,
                         cont_func, (void *)k);
  }

  else if (ms->type == MsgType::Read) {
    void *dst = (void *)(ms->ptr + (seq - ms->pip) * BLOCKSIZE);
    // better_memcpy(dst, msg + 1, BLOCKSIZE);
    memcpy(ReadBuffer, dst, BLOCKSIZE); // 每次都是拿这个buffer来进行copy
    if (seq >= ms->tot) {
      auto &respbuf = req_handle->pre_resp_msgbuf_;
      rpc->enqueue_response(req_handle, &respbuf);
      return;
    }

    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *sdmsg = reinterpret_cast<CommonMsg *>(req.buf_);
    sdmsg->type = MsgType::Read;
    auto &ctx = sdmsg->u.Request;
    ctx.bid = ms->bid + seq;
    ctx.ms = (uint64_t)ms;
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg));
    rpc->enqueue_request(session, kReqType, &req, &resp,
                         cont_func, (void *)k);
  }

  auto &respbuf = req_handle->pre_resp_msgbuf_;
  rpc->enqueue_response(req_handle, &respbuf);
}

int dbs_write(uint64_t blockID, void *srcBuf, uint64_t len) { // len是包大小
  // 创建回调任务
  Mission *ms = new Mission;
  ms->type = MsgType::Write;
  ms->tot = len;
  ms->bid = blockID;
  ms->ptr = (uint64_t)srcBuf;
  ms->pip = ms->nxt = std::min(PIPLINE_PKT_DEPTH, len);

  for (int i = 0; i < ms->nxt; i++) {
    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
    msg->type = MsgType::Write;
    auto &ctx = msg->u.Request;
    ctx.bid = ms->bid + i;
    ctx.ms = reinterpret_cast<uint64_t>(ms);
    void *src = (void *)((uint64_t)srcBuf + i * BLOCKSIZE);
    // better_memcpy(msg + 1, src, BLOCKSIZE);
    memcpy(src, ReadBuffer, BLOCKSIZE); // 每次都是拿这个buffer来进行copy
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg) + BLOCKSIZE);
    rpc->enqueue_request(session, kReqType, &req, &resp,
                         cont_func, (void *)k);
  }

  // 数据请求仍实现为阻塞式
  // while (ms->nxt < ms->tot + ms->pip) rpc->run_event_loop_once();
  while (ms->nxt < ms->tot) {
    cout << ms->nxt << " " << ms->tot << endl;
    rpc->run_event_loop_once();
  }

  cout << "read over\n";
  delete ms;
  return 0;
}

int dbs_read(uint64_t blockID, void *dstBuf, uint64_t len) {
  // 创建回调任务
  Mission *ms = new Mission;
  ms->type = MsgType::Read;
  ms->tot = len;
  ms->bid = blockID;
  ms->ptr = (uint64_t)dstBuf;
  ms->pip = ms->nxt = std::min(PIPLINE_PKT_DEPTH, len);

  for (int i = 0; i < ms->nxt; i++) {
    int64_t k = avail.front();
    avail.pop();
    erpc::MsgBuffer &req = reqs[k];
    erpc::MsgBuffer &resp = resps[k];
    CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
    msg->type = MsgType::Read;
    auto &ctx = msg->u.Request;
    ctx.bid = ms->bid + i;
    ctx.ms = reinterpret_cast<uint64_t>(ms);
    // ctx.ptr = (uint64_t)dstBuf + i * BLOCKSIZE;
    rpc->resize_msg_buffer(&req, sizeof(CommonMsg));
    rpc->enqueue_request(session, kReqType, &req, &resp,
                         cont_func, (void *)k);
  }

  // 数据请求仍实现为阻塞式
  while (ms->nxt < ms->tot + ms->pip) rpc->run_event_loop_once();
  delete ms;
  return 0;
}

void Init(string filename) {
	ifstream fin(filename);
	int id, key;
	string op;
	int GetCnt = 0;
	vector<Request> requests_vec;
	while (fin >> id >> op >> key) {
    if (key + OneRequestContainBlockSize - 1 > 10000000 - 1) { // 控制一下块号范围
      key = 10000000 - 1 - OneRequestContainBlockSize + 1; // 进行修改
    }
		Request request;
		if (op == "read") {
			request.req_type = kRead;
			request.blockID = key;
			request.ops_id = id;
			requests_vec.push_back(request);
			// Requests.push(request);
		} 
		else if (op == "scan") {
			/* do nothing */
		} 
		else if (op == "update") {
			request.req_type = kWrite;
			request.blockID = key;
			request.ops_id = id;
			requests_vec.push_back(request);
			// Requests.push(request);
		} 
		else if (op == "insert") {
			request.req_type = kWrite;
			request.blockID = key;
			request.ops_id = id;
			requests_vec.push_back(request);
			// Requests.push(request);
		} 
		else if (op == "delete") {
			/* do nothing */
		}
		GetCnt ++;
		if (GetCnt == RequestTotNumber) { // 另外一种结束方式
			break;
		}
	}
	fin.close();

	for (int i = 0; i < RequestTotNumber; i++) {	// std::random_device rd;
		Requests.push(requests_vec[i]);
	}
	cout << "Client init OK" << endl;
}

void Run() {
	auto start = std::chrono::high_resolution_clock::now();
    while (Requests.size()) {
        Request request = Requests.front();
        Requests.pop();
        if (request.req_type == kRead) {
            dbs_read(request.blockID, nullptr, OneRequestContainBlockSize);
        }
        else if (request.req_type == kWrite) {
            dbs_write(request.blockID, nullptr, OneRequestContainBlockSize);
        }
    }
	auto end = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  // auto second = duration.count() / 1000000;
	cout << "Time: " << duration.count() << "us" << endl;
	cout << "IOPS: " << 1000000.0 * RequestTotNumber / duration.count()  << "ops/s" << endl;
	cout << "throughput: " << 1000000.0 * RequestTotNumber * BLOCKSIZE / 1024 * OneRequestContainBlockSize / duration.count()  / 1024 << "MB/s" << endl;
	cout << "if use 4096, the throughput is: " << 1000000.0 * RequestTotNumber * 4096  / 1024 * OneRequestContainBlockSize / duration.count()  / 1024 << "MB/s" << endl;
}

void client(string clientip, string serverip) {
	nexus = new erpc::Nexus(clientip.c_str());
  cout << "Client object" << endl;
	// nexus.register_req_func(kReqType, req_handler); // NOTE 现在用不上这个
	
	rpc = new erpc::Rpc<erpc::CTransport>(nexus, nullptr, 0, sm_handler);
	for (int i = 0; i < N; i++) {
		avail.push(i);
		reqs[i] = rpc->alloc_msg_buffer_or_die(MAX_MSG_SIZE);
		resps[i] = rpc->alloc_msg_buffer_or_die(MAX_MSG_SIZE);
	}
	session = rpc->create_session(serverip.c_str(), 0);
	while (!rpc->is_connected(session)) rpc->run_event_loop_once();
	cout << "Client connected to server" << endl;

  // rpc->run_event_loop(3000); // NOTE 用于等待server的连接
}

int main(int argc, char* argv[]) {
	if (argc != 6) {
        cerr << "Usage: " << argv[0] << " <clientip> <serverip> <workloadType> <RequestTotNumber> <OneRequestContainBlockSize>" << endl;
        return 1;
    }
	string clientip = argv[1];
  string serverip = argv[2];
	string fileType = argv[3];
	RequestTotNumber = atoi(argv[4]);
  OneRequestContainBlockSize = atoi(argv[5]);

	client(clientip, serverip);
	string workload = "./workload/workload" + fileType + ".txt";
	Init(workload);
	Run();
	return 0;
}