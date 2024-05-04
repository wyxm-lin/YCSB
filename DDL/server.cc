#include <sys/mman.h>
#include <iostream>
#include <queue>
#include "common.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>

#define Usefile
// #define UseBuffer
    
using namespace std;

int fd;
struct stat sb;
string db_name = "/home/linguangming/wyxm/JusteRPC/xihu/disk/db.txt"; // 这个在14号机
void *blk;

int session = -1;
erpc::Rpc<erpc::CTransport> *rpc;
std::queue<int64_t> avail;
erpc::MsgBuffer reqs[MSG_BUFFER_NUM];
erpc::MsgBuffer resps[MSG_BUFFER_NUM];
char BUFFER[BLOCKSIZE];

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
    blk = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, fd, 0);
    if (blk == MAP_FAILED) {
        cerr << "Error mapping the file with huge pages" << endl;
        close(fd);
        exit(1);
    }
    cout << "mmap OK\n";
    // memset(blk, 0, sb.st_size - 1);
    
    char* check = (char*)blk;
    for (int i = 0; i < 10; i++) {
        cout << check[i];
    }
    cout << "\nCheck OK\n";
}

void cont_func(void *, void *use) {
    int64_t used = (int64_t)use;
    avail.push(used);
}

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {}

void req_handler(erpc::ReqHandle *req_handle, void *) {
    auto *msg = reinterpret_cast<CommonMsg *>(req_handle->get_req_msgbuf()->buf_);
    if (msg->req_type == RequestType::kRead) {
        auto &resp = req_handle->pre_resp_msgbuf_;
#ifdef Usefile   
        char* src = (char*)blk + msg->blockID * BLOCKSIZE;
        memcpy(resp.buf_, src, BLOCKSIZE); // 从mmap的文件中读取数据
        char *last = (char *)resp.buf_ + BLOCKSIZE - 1;
        *last = '\0'; // 最后一个字节填充为'\0' (好像不需要这样)
#endif
#ifdef UseBuffer
        memcpy(resp.buf_, BUFFER, BLOCKSIZE); // 从内存中读取数据
#endif
        rpc->enqueue_response(req_handle, &resp);
    } 
    else if (msg->req_type == RequestType::kWrite) {
#ifndef NotUseDisk    
        char* dst = (char*)blk + msg->blockID * BLOCKSIZE;
        memcpy(dst, msg + 1, BLOCKSIZE); // 将数据写入mmap的文件中
#endif
#ifdef NotUseDisk
        memcpy(BUFFER, msg + 1, BLOCKSIZE); // 从内存中读取数据
#endif
        auto &resp = req_handle->pre_resp_msgbuf_;
        rpc->enqueue_response(req_handle, &resp);
        // mtx[msg->blockID].unlock();
    } else if (msg->req_type == RequestType::kEmpty) {
        // mylog("server empty\n");
        auto &resp = req_handle->pre_resp_msgbuf_;
        rpc->enqueue_response(req_handle, &resp);
    } else {
        exit(-1);
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " <serverip>" << endl;
        return 1;
    }

    string ip = argv[1];
    InitDB();
    cout << "server storage OK\n";
    
    erpc::Nexus nexus(ip.c_str());
    nexus.register_req_func(kReqType, req_handler);
    rpc = new erpc::Rpc<erpc::CTransport>(&nexus, nullptr, 0, sm_handler);
    for (int i = 0; i < MSG_BUFFER_NUM; i++) {
        avail.push(i);
        reqs[i] = rpc->alloc_msg_buffer_or_die(MAX_MSG_SIZE);
        resps[i] = rpc->alloc_msg_buffer_or_die(MAX_MSG_SIZE);
    }

    rpc->run_event_loop(1000000);
    delete rpc;
    return 0;
}
/*
sudo ./server 10.10.10.14:31870
*/