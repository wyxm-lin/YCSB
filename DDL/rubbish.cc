#include <queue>
#include <atomic>
#include "common.h"
#include <fstream>
#include <cstdlib>
#include <chrono>
#include <random>
#include <vector>

using namespace std;

int OneRequestContainBlockSize;
const int SyncNumberMax = 128;
int SyncNumber = 16; // 16个请求一起
#ifdef debug
const int RequestTotNumber = 100;
#endif
#ifndef debug
const int RequestTotNumber = 10000000; // 用于从文件中加载一定数量的请求
#endif

/*****************一个连接需要的变量**********************/
uint32_t session;
static erpc::Rpc<erpc::CTransport> *rpc;
static erpc::MsgBuffer reqs[MSG_BUFFER_NUM];
static erpc::MsgBuffer resps[MSG_BUFFER_NUM];
static Callback cb[MSG_BUFFER_NUM];
static queue<int64_t> avail;
static char SharedBuffer[SyncNumberMax][BLOCKSIZE];
static queue<int> avail_sync_buffer_id;
static queue<Request> Requests;
static int DoneNumber = 0;
/******************************************************/

// 直接设置为每一个client都有一个专属的任务队列 以及 以流水深度为长度的buffer
// (为什么要这样设计?
// 第一点,因为对于每个请求，我不好控制push进去这个队列)
// 第二点,此处没有数据的概念

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

void req_handler(erpc::ReqHandle *req_handle, void *) {
	auto &respmsg = req_handle->pre_resp_msgbuf_;
	rpc->enqueue_response(req_handle, &respmsg);
}

void cont_func(void *, void *cbctx) {
	// Step1: 得出该请求使用的req resp cb的index
	int64_t avail_index = (int64_t)cbctx;
	avail.push(avail_index); // NOTE 回收!!!
	mylog("avail_index: %ld\n", avail_index);

	// Step2: 处理信息
	erpc::MsgBuffer &req = reqs[avail_index];
	erpc::MsgBuffer &resp = resps[avail_index];
	CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
	mylog("get the %dth response\n", msg->ops_id);
	DoneNumber ++; // 完成的任务 ++
	if (msg->req_type == kEmpty) {
		/* do nothing */
		mylog("cont_func get empty responce\n");
	}
	else if (msg->req_type == kRead) {
		mylog("cont_func get read responce\n");
		int use_shared_buffer_id = cb[avail_index].use_shared_buffer_id; // 读数据也只是用一个shared的位置进行读取
		mylog("use_shared_buffer_id: %d\n", use_shared_buffer_id);
		void* dst = (void*)SharedBuffer[use_shared_buffer_id];
		memcpy(dst, (void*)resp.buf_, BLOCKSIZE); // 实际数据的copy (读请求的responce只有数据)
		mylog("read response copy over\n");
		avail_sync_buffer_id.push(use_shared_buffer_id); // NOTE 回收sync_buffer_id
	}
	else if (msg->req_type == kWrite) {
		/* do nothing */
		mylog("cont_func get write responce\n");
	}
	else {
		exit(-1);
	}

	// Step3: 判断负载是否完全结束
	if (Requests.empty()) {
		return;
	}

	mylog("cont_func will send next request\n");
	// Step4: 发送下一个请求
	{
		int64_t k = avail.front();
		avail.pop();
		erpc::MsgBuffer &req = reqs[k];
		erpc::MsgBuffer &resp = resps[k];

		// 取出请求(此处保证队列长度一定大于SyncNumber)
		Request request;
		request = Requests.front();
		Requests.pop();

		// 设置cb
		if (request.req_type == kRead) {
			int sync_buffer_id = avail_sync_buffer_id.front();
			avail_sync_buffer_id.pop();	
			cb[k].use_shared_buffer_id = sync_buffer_id;
		}
		else {
			cb[k].use_shared_buffer_id = -1;
		}

		// 修改req,即填充commonmsg
		CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
		msg->req_type = request.req_type;
		msg->blockID = request.blockID;
		msg->ops_id = request.ops_id;
		if (request.req_type == kWrite) {
			char buftmp[BLOCKSIZE]; // 写请求的写数据 只是用一个临时的数据生成
			memset(buftmp, 'a', BLOCKSIZE);
			buftmp[BLOCKSIZE - 1] = '\0';
			memcpy(msg + 1, (void *)buftmp, BLOCKSIZE); // write拷贝数据(此处没有考虑数据的来源问题)
		}
		rpc->resize_msg_buffer(&req, sizeof(CommonMsg) + BLOCKSIZE);

		// 加入到rpc的队列中
		mylog("send the %dth request", request.ops_id);
		// int functionType = rand() % BackgroundThreadNum;
		rpc->enqueue_request(session, kReqType, &req, &resp, cont_func, (void *)k);
	}
}

class Client {
private:
	erpc::Nexus nexus;

public:
	Client(string clientip, string serverip);
	~Client();

	void Init(string filename);
	void Run();
};

Client::Client(string clientip, string serverip) : nexus(clientip.c_str()) {
	cout << "Client object" << endl;
	// nexus.register_req_func(kReqType, req_handler); // NOTE 现在用不上这个
	
	rpc = new erpc::Rpc<erpc::CTransport>(&nexus, nullptr, 0, sm_handler);
	for (int i = 0; i < MSG_BUFFER_NUM; i++) {
		avail.push(i);
		reqs[i] = rpc->alloc_msg_buffer_or_die(MAX_MSG_SIZE);
		resps[i] = rpc->alloc_msg_buffer_or_die(MAX_MSG_SIZE);
	}
	for (int i = 0; i < SyncNumber; i++) {
		avail_sync_buffer_id.push(i);
	}
	session = rpc->create_session(serverip.c_str(), 0);
	while (!rpc->is_connected(session)) rpc->run_event_loop_once();
	cout << "Client connected to server" << endl;
}

Client::~Client() {
	delete rpc;
	rpc = nullptr;
}

void Client::Init(string filename) {
	ifstream fin(filename);
	int id, key;
	string op;
	int GetCnt = 0;
	vector<Request> requests_vec;
	while (fin >> id >> op >> key) {
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
	// std::mt19937 g(rd());
	// std::shuffle(requests_vec.begin(), requests_vec.end(), g);
		Requests.push(requests_vec[i]);
	}
	cout << "Client init OK" << endl;
}

void Client::Run() {
	auto start = std::chrono::high_resolution_clock::now();
	for (int i = 1; i <= SyncNumber; i++) {
		// 取出k
		int64_t k = avail.front();
		avail.pop();
		erpc::MsgBuffer &req = reqs[k];
		erpc::MsgBuffer &resp = resps[k];

		// 取出请求(此处保证队列长度一定大于SyncNumber)
		Request request;
		request = Requests.front();
		Requests.pop();

		// 设置cb
		if (request.req_type == kRead) {
			int sync_buffer_id = avail_sync_buffer_id.front();
			avail_sync_buffer_id.pop();	
			cb[k].use_shared_buffer_id = sync_buffer_id;
		}
		else {
			cb[k].use_shared_buffer_id = -1;
		}

		// 修改req,即填充commonmsg
		CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
		msg->req_type = request.req_type;
		msg->blockID = request.blockID;
		msg->ops_id = request.ops_id;
		if (request.req_type == kWrite) {
			char buftmp[BLOCKSIZE];
			memset(buftmp, 'a', BLOCKSIZE);
			buftmp[BLOCKSIZE - 1] = '\0';
			memcpy(msg + 1, (void *)buftmp, BLOCKSIZE); // write拷贝数据(此处没有考虑数据的来源问题)
		}
		rpc->resize_msg_buffer(&req, sizeof(CommonMsg) + BLOCKSIZE);

		// 加入到rpc的队列中
		mylog("send the %dth request", request.ops_id);
		// int functionType = rand() % BackgroundThreadNum;
		rpc->enqueue_request(session, kReqType, &req, &resp, cont_func, (void *)k);
	}
	while (true) {
		if (DoneNumber == RequestTotNumber) { // 这个才是结束条件
			break;
		}
		rpc->run_event_loop_once();
	}
	auto end = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
	cout << "Time: " << duration.count() << "us" << endl;
	cout << "IOPS: " << 1000000.0 * RequestTotNumber / duration.count() << "ops/s" << endl;
	cout << "throughput: " << 1000000.0 * RequestTotNumber * BLOCKSIZE / duration.count() / 1024 / 1024 << "MB/s" << endl;
	cout << "if use 4096, the throughput is: " << 1000000.0 * RequestTotNumber * 4096 / duration.count() / 1024 / 1024 << "MB/s" << endl;
}

void doRead(int RealKey) {
    
}

void doWrite() {

}

int main(int argc, char* argv[]) {
	if (argc != 6) {
        cerr << "Usage: " << argv[0] << " <clientip> <serverip> <workloadType> <SyncNumeber> <OneRequestContainBlockSize>" << endl;
        return 1;
    }
	string clientip = argv[1];
    string serverip = argv[2];
	string fileType = argv[3];
	SyncNumber = atoi(argv[4]);
    OneRequestContainBlockSize = atoi(argv[5]);

	Client client(clientip, serverip);
	string workload = "./workload/workload" + fileType + ".txt";
	client.Init(workload);
	client.Run();
	return 0;
}

void doRead(int RealKey, int ops_id) {
    int RealSyncNumber = min(SyncNumber, OneRequestContainBlockSize);
    TOTAl = OneRequestContainBlockSize; // 设置这个
    for (int i = 1; i <= RealSyncNumber; i++) {
        // 取出K
        int64_t k = avail.front();
        avail.pop();
        erpc::MsgBuffer &req = reqs[k];
        erpc::MsgBuffer &resp = resps[k];

        // 设置回调
        int sync_buffer_id = avail_sync_buffer_id.front(); // 读请求用于存储数据
        avail_sync_buffer_id.pop();
        cb[k].use_shared_buffer_id = sync_buffer_id;
        cb[k].tot = OneRequestContainBlockSize; // 设置这个请求包的总大小
        cb[k].now = i; // 目前是第几个包
        cb[k].Type = kRead;

        // 填充commonmsg
        CommonMsg *msg = reinterpret_cast<CommonMsg *>(req.buf_);
        msg->ops_id = ops_id;
        msg->req_type = kRead;
        msg->blockID = RealKey + (i - 1) * BLOCKSIZE; // blockid修改
        rpc->resize_msg_buffer(&req, sizeof(CommonMsg)); // 读的时候只需要这样 不需要+BLOCKSIZE
        
        // 加入队列
        rpc->enqueue_request(session, kReqType, &req, &resp, cont_func, (void *)k);
    }
    while (TOT) {
        rpc->run_event_loop_once();
    }
}