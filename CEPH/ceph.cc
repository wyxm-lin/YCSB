/*
g++ -o ceph ceph.cc -lrados -lrbd -lpthread
*/
#include "/usr/include/rados/librados.h"
#include "/usr/include/rbd/librbd.h"
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <stdbool.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std;

enum RequestType { kRead = 0, kWrite, kEmpty };
typedef struct Request {
  RequestType req_type; // 请求类型
  uint64_t blockID;     // 块号
  uint64_t ops_id;      // 操作号
} Request;

const int ThreadNumberMax = 1024;
int ThreadNumber = 1;                 // 线程数
const int BlockRange = 10000000;            // 块号范围
int LoadRequestNumber = 100;     // 从文件中加载请求数量
const int CEPHBLOCKSIZE = 4096;             // Ceph块大小
queue<Request> RequestsQueue[ThreadNumberMax]; // 请求队列
std::atomic<bool> BlockID[BlockRange]; // 判断该块号是否被占用 保证安全性
int OneRequestContainBlockSize;

// 用于最后同步
std::atomic<int> async_op_count{0};
std::mutex async_op_mutex;
std::condition_variable async_op_cv;

// 集群相关变量
std::mutex rbd_mutex;
rados_t cluster;
rados_ioctx_t io;
rbd_image_t image;

struct Arg {
  char *buffer;
  int blockId;
};

// 异步读回调
void read_aio_completion_callback(rbd_completion_t cb, void *arg) {
  struct Arg *arg_ptr = (struct Arg *)arg;
  char *buf = arg_ptr->buffer;
  int blockId = arg_ptr->blockId;
  BlockID[blockId].store(false); // 设置为未占用
  // cout << "read_aio_completion_callback " << blockId << endl;
  int ret = rbd_aio_get_return_value(cb);
  rbd_aio_release(cb);
  free(buf);
  async_op_count--;
  async_op_cv.notify_all();
}

// 异步写任务没有必要等待
void update_aio_completion_callback(rbd_completion_t cb, void *arg) {
  struct Arg *arg_ptr = (struct Arg *)arg;
  int blockId = arg_ptr->blockId;
  BlockID[blockId].store(false); // 设置为未占用
  // cout << "update_aio_completion_callback " << blockId << endl;
  int ret = rbd_aio_get_return_value(cb);
  rbd_aio_release(cb);
}

// 等待所有的异步读任务
void wait_for_all_async_ops() { // 只需要等待所有的异步读操作
  std::unique_lock<std::mutex> lock(async_op_mutex);
  async_op_cv.wait(lock, [] { return async_op_count == 0; });
}

// 读请求
void Read(int RealKey) {
  // cout << "read " << RealKey << endl;
  // for (int i = 1; i <= OneRequestContainBlockSize; i++) {
  //   char* buff = (char *)malloc(CEPHBLOCKSIZE);  // 申请内存
  //   struct Arg *arg = (struct Arg *)malloc(sizeof(struct Arg));
  //   arg->buffer = buff;
  //   arg->blockId = RealKey + i - 1;
  //   long long offset = 1LL * (RealKey + i - 1) * CEPHBLOCKSIZE;
  //   rbd_completion_t comp;
  //   rbd_aio_create_completion(arg, read_aio_completion_callback, &comp);
  //   std::unique_lock<std::mutex> rbd_lock(rbd_mutex);
  //   rbd_aio_read(image, offset, CEPHBLOCKSIZE, buff, comp);
  //   async_op_count++;
  //   rbd_lock.unlock();
  // }
  // cout << "read " << RealKey << endl;
  char *buff = (char *)malloc(CEPHBLOCKSIZE * OneRequestContainBlockSize);  // 申请内存
  struct Arg *arg = (struct Arg *)malloc(sizeof(struct Arg));
  arg->buffer = buff;
  arg->blockId = RealKey;
  long long offset = 1LL * RealKey * CEPHBLOCKSIZE;
  rbd_completion_t comp;
  rbd_aio_create_completion(arg, read_aio_completion_callback, &comp);
  
  // cout << "read lock\n";
  std::unique_lock<std::mutex> rbd_lock(rbd_mutex);
  rbd_aio_read(image, offset, CEPHBLOCKSIZE * OneRequestContainBlockSize, buff, comp);
  async_op_count++;
  rbd_lock.unlock();
  // cout << "read unlock\n";
}

void Update(int RealKey) {
  // cout << "update " << RealKey << endl;
  // for (int i = 1; i <= OneRequestContainBlockSize; i++) {
  //   char buf[CEPHBLOCKSIZE];
  //   memset(buf, 'a', CEPHBLOCKSIZE); // 注意此处需要占用写4096的时间
  //   buf[CEPHBLOCKSIZE - 1] = '\0';
  //   long long offset = 1LL * (RealKey + i - 1) * CEPHBLOCKSIZE;
  //   struct Arg *arg = (struct Arg *)malloc(sizeof(struct Arg));
  //   arg->blockId = RealKey + i - 1;
  //   rbd_completion_t comp;
  //   rbd_aio_create_completion(arg, update_aio_completion_callback, &comp);
  //   std::unique_lock<std::mutex> rbd_lock(rbd_mutex);
  //   rbd_aio_write(image, offset, CEPHBLOCKSIZE, buf, comp);
  //   rbd_lock.unlock();
  // }
  // cout << "update " << RealKey << endl;
  char buf[CEPHBLOCKSIZE * OneRequestContainBlockSize];
  memset(buf, 'a', CEPHBLOCKSIZE * OneRequestContainBlockSize); // 注意此处需要占用写4096的时间
  buf[CEPHBLOCKSIZE - 1] = '\0';
  long long offset = 1LL * RealKey * CEPHBLOCKSIZE;
  struct Arg *arg = (struct Arg *)malloc(sizeof(struct Arg));
  arg->blockId = RealKey;
  rbd_completion_t comp;
  rbd_aio_create_completion(arg, update_aio_completion_callback, &comp);
  std::unique_lock<std::mutex> rbd_lock(rbd_mutex);
  rbd_aio_write(image, offset, CEPHBLOCKSIZE * OneRequestContainBlockSize, buf, comp);
  // rbd_write(image, offset, CEPHBLOCKSIZE * OneRequestContainBlockSize, buf);
  rbd_lock.unlock();
}

void Close() {
  cout << "enter Close\n";
  rbd_close(image);
  rados_ioctx_destroy(io);
  rados_shutdown(cluster);
  cout << "close OK\n";
}

void RealCephDB(int PoolId, int ImageId) {
  std::cout << "High resolution clock period: "
            << std::chrono::high_resolution_clock::period::num << "/"
            << std::chrono::high_resolution_clock::period::den << " seconds"
            << std::endl;

  const char *clustername = "32d0f3a2-07af-11ef-a63b-b7b87dd43c5a";

  int err = rados_create(&cluster, NULL);
  if (err < 0) {
    cerr << "Couldn't create the cluster handle! error " << err << std::endl;
    exit(1);
  } else {
    cout << "Created a cluster handle." << std::endl;
  }

  err = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
  if (err < 0) {
    cerr << "Cannot read config file: " << err << std::endl;
    exit(1);
  } else {
    cout << "Read the config file." << std::endl;
  }

  err = rados_connect(cluster);
  if (err < 0) {
    cerr << "Cannot connect to cluster: " << err << std::endl;
    exit(1);
  } else {
    cout << "Connected to the cluster." << std::endl;
  }

  string poolname = "pool";
  poolname += std::to_string(PoolId);

  err = rados_ioctx_create(cluster, poolname.c_str(), &io);
  if (err < 0) {
    cerr << "Cannot open rados pool: " << err << std::endl;
    rados_shutdown(cluster);
    exit(1);
  } else {
    cout << "Created I/O context." << std::endl;
  }

  string image_name = "image";
  image_name += std::to_string(ImageId);
  image_name += ".img";
  err = rbd_open(io, image_name.c_str(), &image, NULL);
  if (err < 0) {
    cerr << "Cannot open rbd image: " << err << std::endl;
    rados_ioctx_destroy(io);
    rados_shutdown(cluster);
    exit(1);
  } else {
    cout << "Opened rbd image " << image_name << "." << std::endl;
  }

  cout << "db file opened." << endl;

  for (int i = 0; i < BlockRange; i++) {
    BlockID[i].store(false);
  }
  cout << "BlockID init OK" << endl;
}

void RealCephDBover() { cout << "db file closed." << endl; }

void Init(string filename) {
  ifstream fin(filename);
  int id, key;
  string op;
  int GetCnt = 0;
  vector<Request>vec;
  while (fin >> id >> op >> key) {
    Request request;
    if (op == "read") {
      request.req_type = kRead;
      request.blockID = key;
      request.ops_id = id;
      vec.push_back(request);
    } else if (op == "scan") {
      /* do nothing */
    } else if (op == "update") {
      request.req_type = kWrite;
      request.blockID = key;
      request.ops_id = id;
      vec.push_back(request);
    } else if (op == "insert") {
      request.req_type = kWrite;
      request.blockID = key;
      request.ops_id = id;
      vec.push_back(request);
    } else if (op == "delete") {
      /* do nothing */
    }
    GetCnt++;
    if (GetCnt == LoadRequestNumber) { // 另外一种结束方式
      break;
    }
  }
  fin.close();
  cout << "Client init OK" << endl;
  for (int i = 0; i < vec.size(); i++) {
    RequestsQueue[i % ThreadNumber].push(vec[i]); // 将负载均衡的请求分配到不同的线程
  }
}

void Run(int thread_id) {
  while (RequestsQueue[thread_id].size()) {
    Request request;
    request = RequestsQueue[thread_id].front();
    RequestsQueue[thread_id].pop();
    int blockID = request.blockID;
    // cout << "Thread " << thread_id << " get blockID " << blockID << " type is " << request.req_type << endl;
    while (BlockID[blockID].load() == true) { // 只管首地址
      continue;
    }
    BlockID[blockID].store(true);
    if (request.req_type == kRead) {
      Read(request.blockID);
    } else if (request.req_type == kWrite) {
      Update(request.blockID);
    }
  }
}

int main(int argc, char *argv[]) {
  if (argc != 7) {
    cerr << "Usage: " << argv[0] << " <PoolId> <ImageId> <WorkloadType> <ThreadNumber> <LoadNumber> <OneRequestContainBlockSize>" << endl;
    return 1;
  }

  int PoolId = atoi(argv[1]);
  int ImageId = atoi(argv[2]);
  string fileTyep = argv[3];
  ThreadNumber = atoi(argv[4]);
  LoadRequestNumber = atoi(argv[5]);
  OneRequestContainBlockSize = atoi(argv[6]);

  string filename = "./workload/workload";
  filename += fileTyep;
  filename += ".txt";
  RealCephDB(PoolId, ImageId);
  Init(filename);

  auto start = std::chrono::high_resolution_clock::now();
  thread t[ThreadNumber];
  for (int i = 0; i < ThreadNumber; i++) {
    t[i] = thread(Run, i);
  }
  for (int i = 0; i < ThreadNumber; i++) {
    t[i].join();
  }
  wait_for_all_async_ops();
  auto end = std::chrono::high_resolution_clock::now();
  double AllDuration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  double Times = 1000000;
  cout << "do the async and C++ thread\n";
  cout << "AllDuration: " << AllDuration / Times << "s" << endl;
  double speed = Times * LoadRequestNumber / AllDuration / 1000;
  cout << "speed: " << speed << " KTPS" << endl;
  cout << "bandwidth: " << LoadRequestNumber << " * "
       << OneRequestContainBlockSize << " * "
       << "4KB / 1024 / " << AllDuration / Times << " = "
       << 1.0 * LoadRequestNumber * OneRequestContainBlockSize * 4 * Times / 1024 / AllDuration << " MB/s" << endl;
  cout << "===================================================================\n";

  // HandleAllData();
  Close();
  RealCephDBover();
  return 0;
}
