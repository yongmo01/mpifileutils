// pfs_migrate.c
// mpicc -O2 -std=c11 -o pfs_migrate pfs_migrate.c
// mpirun -n 12 ./pfs_migrate /path/to/root 8
// 设计目标：MPI流水线——Producer并行遍历→按布局/条带对齐分片→按OST投递到队列Owner→Consumer按主队列优先拉取（空则轮询/窃取）
// 参考动机与目标：流水线化降低“两阶段”内存峰值与MDS瞬时冲击；布局感知+条带对齐分片提升吞吐并减少跨OST抖动 [1][2]

#define _XOPEN_SOURCE 700
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <stdbool.h>
#include <limits.h>
#include <time.h>

enum { TAG_TASK_PUT=1000, TAG_GET_REQ=1001, TAG_GET_RESP=1002, TAG_FIN_PROD=1003, TAG_DONE=1004 };

// 任务类型：小文件（可聚合）与大文件条带分片
typedef enum { TASK_SMALL_BATCHABLE=1, TASK_LARGE_STRIPED_CHUNK=2 } task_kind_t;

typedef struct {
  int   stripe_count;      // 条带数
  int   stripe_size;       // 字节
  int   dominant_ost;      // 主导OST（用于路由队列）
  int   start_ost;         // 起始OST（Lustre条带起点，假布局下为哈希）
} layout_t; 

typedef struct {
  task_kind_t kind;
  char  path[1024];        // 路径（简化：限制长度）
  uint64_t size;           // 小文件：文件大小；分片：chunk大小
  uint64_t offset;         // 小文件：0；分片：chunk起始偏移
  layout_t layout;         // 条带/主导信息
  char  pack_key[256];     // 小文件聚合键（目录+OST），此原型未在消费者端打包，仅示意
  uint32_t flags;          // 例如 CHUNK_ALIGN_TO_STRIPE = 1
  uint32_t md_ops_hint;    // 估算元数据操作数
} task_t;

// 简易环形队列（队列所有者rank内部使用；单线程访问，无锁）
typedef struct {
  task_t *buf;
  int capacity;
  int head;   // 出队位置
  int tail;   // 入队位置
  int size;   // 当前元素数
} ringq_t;

static void rq_init(ringq_t* q, int cap) {
  q->buf = (task_t*)malloc(sizeof(task_t)*cap);
  q->capacity = cap; q->head=0; q->tail=0; q->size=0;
}                                                                                                                                                                                                                                     
static void rq_free(ringq_t* q){ free(q->buf); q->buf=NULL; }
static int  rq_full(ringq_t* q){ return q->size==q->capacity; }
static int  rq_empty(ringq_t* q){ return q->size==0; }
static int  rq_push(ringq_t* q, const task_t* t){
  if (rq_full(q)) return 0;
  q->buf[q->tail]=*t; q->tail=(q->tail+1)%q->capacity; q->size++; return 1;
}
static int  rq_pop(ringq_t* q, task_t* t){
  if (rq_empty(q)) return 0;
  *t = q->buf[q->head]; q->head=(q->head+1)%q->capacity; q->size--; return 1;
}

// 简易哈希（用于目录划分与“假布局”的起始OST）
static uint64_t djb2(const char* s){ 
  uint64_t h=5381; 
  int c; 
  while((c=*s++)) h=((h<<5)+h)+ (uint8_t)c; 
  return h; 
}

// 目录分工：无集中协调，基于路径哈希把子树静态映射到各Producer，避免重复遍历与通信
static int assign_dir_to_producer(const char* path, int numP, int myPIndex){
  if (numP<=1) return 1;
  uint64_t h = djb2(path);//对路径计算出64位的哈希值
  int owner = (int)(h % (uint64_t)numP);//计算出该路径所属的Producer编号
  return owner==myPIndex;//返回1代表该Producer应该负责该路径；否则返回0
}

// 近似Lustre“轮询”布局（无Lustre头文件时的可运行版本）
// 真实环境可替换为 ioctl/llapi_file_get_stripe/xattr 获取条带信息
static layout_t fake_layout_for(const char* path, uint64_t fsize, int num_ost, int def_stripe_sz, int def_stripe_cnt){
  layout_t L = {0};
  L.stripe_size  = def_stripe_sz;
  L.stripe_count = def_stripe_cnt>0 ? def_stripe_cnt : 1;
  if (L.stripe_count > num_ost) L.stripe_count = num_ost;
  L.start_ost = (int)(djb2(path) % (uint64_t)num_ost);
  // 对小文件：主导OST设为 start_ost；对分片：会按offset计算
  L.dominant_ost = L.start_ost;
  return L;
}

// 计算某个偏移的主导OST（条带轮询）
static int ost_for_offset(const layout_t* L, int num_ost, uint64_t off){
  if (L->stripe_count<=1) return L->start_ost;
  uint64_t stripe_idx = (off / (uint64_t)L->stripe_size) % (uint64_t)L->stripe_count;
  int ost = (L->start_ost + (int)stripe_idx) % num_ost;
  return ost;
}

/* 自适配角色划分（可通过环境覆盖）：给定world_size与num_ost */
/* Producer ranks（P）; Queue-owner ranks（Q）; Consumer ranks（C）; */
typedef struct {
  int numP, numQ, numC;        // 各角色数量
  int baseP, baseQ, baseC;     // 各角色起始rank（连续区间）
} role_plan_t;

static int env_int(const char* k, int dflt){
  const char* v=getenv(k); if (!v) return dflt; char* e; long x=strtol(v,&e,10); return (e==v)?dflt:(int)x;
}

static role_plan_t plan_roles(int world, int num_ost){
  // 默认策略：Q = min(num_ost, max(1, world/8)); P = max(1, (world - Q)/4); C = world - P - Q
  // 可通过环境覆盖：PFS_NUM_P / PFS_NUM_Q / PFS_NUM_C
  role_plan_t rp={0};
  int envP=env_int("PFS_NUM_P",-1), envQ=env_int("PFS_NUM_Q",-1), envC=env_int("PFS_NUM_C",-1);
  if (envP>0 && envQ>0 && envC>0 && envP+envQ+envC==world){
    rp.numP=envP; rp.numQ=envQ; rp.numC=envC;
  }else{
    rp.numQ = num_ost<world? num_ost : (world>8? world/8:1);
    if (rp.numQ<1) rp.numQ=1; 
    if (rp.numQ>world-2) rp.numQ=world-2;
    rp.numP = (world - rp.numQ)/4; 
    if (rp.numP<1) rp.numP=1;
    rp.numC = world - rp.numP - rp.numQ; 
    if (rp.numC<1){ 
      rp.numC=1; 
      if(rp.numP>1) rp.numP--; 
    } 
  }
  rp.baseP=0; rp.baseQ=rp.baseP+rp.numP; rp.baseC=rp.baseQ+rp.numQ;
  return rp;
}

static int is_producer(int rank, role_plan_t rp){ return rank>=rp.baseP && rank<rp.baseP+rp.numP; }
static int is_qowner  (int rank, role_plan_t rp){ return rank>=rp.baseQ && rank<rp.baseQ+rp.numQ; }
static int is_consumer(int rank, role_plan_t rp){ return rank>=rp.baseC && rank<rp.baseC+rp.numC; }

// ost -> 队列所有者rank（多OST映射到少量Q）
static int ost_owner_rank(int ost, role_plan_t rp, int num_ost){
  int idx = ost % rp.numQ;
  return rp.baseQ + idx;
}

// ---------- Producer：遍历 + 任务生成 + 同步发送到对应队列Owner（队列满则阻塞=反压） ----------
/* Producer 配置参数 */
typedef struct {
  const char* root;
  int me, numP, myPIndex;// me=rank;numP=总Producer数;myPIndex=rank-baseP（逻辑上第几个Producer）
  int num_ost;
  int small_thresh;     // <= 128KB 等视为小文件
  int stripe_sz;        // 默认条带大小（假布局）
  int stripe_cnt;       // 默认条带数（假布局）
  int chunk_mb;         // 大文件分片MB，按条带对齐
  role_plan_t rp;
} prod_cfg_t;

/* 将task发送到该OST队列所有者 */
static void ssend_task_to_owner(const task_t* t, role_plan_t rp, int num_ost){
  int dst = ost_owner_rank(t->layout.dominant_ost, rp, num_ost);
  // 同步发送：当队列所有者不接收或队列满时，发送阻塞，实现“反压”，避免无限膨胀
  MPI_Ssend((void*)t, sizeof(task_t), MPI_BYTE, dst, TAG_TASK_PUT, MPI_COMM_WORLD);
}

/* 生成小文件任务 */
static void emit_small_file_task(const char* path, uint64_t fsize, layout_t L, const prod_cfg_t* cfg){
  task_t t={0};
  t.kind = TASK_SMALL_BATCHABLE;
  snprintf(t.path, sizeof(t.path), "%s", path);
  t.size = fsize; t.offset=0;
  t.layout = L;
  // 聚合键（示意）：目录+dominant_ost
  const char* slash = strrchr(path,'/'); size_t dirlen = slash? (size_t)(slash - path) : 0;
  if (dirlen>0 && dirlen<200){ snprintf(t.pack_key, sizeof(t.pack_key), "dir:%.*s|ost:%d",(int)dirlen,path,L.dominant_ost); }
  t.flags = 0; t.md_ops_hint=2;
  ssend_task_to_owner(&t, cfg->rp, cfg->num_ost);
}

/* 生成大文件分片任务 */
static void emit_large_file_chunks(const char* path, uint64_t fsize, layout_t L, const prod_cfg_t* cfg){
  uint64_t chunk = (uint64_t)cfg->chunk_mb*1024ULL*1024ULL;
  if (chunk==0) chunk = (uint64_t)L.stripe_size; // 最小按1条带
  // 保证chunk对齐条带
  if (chunk % (uint64_t)L.stripe_size) chunk += (uint64_t)L.stripe_size - (chunk % (uint64_t)L.stripe_size);
  for (uint64_t off=0; off<fsize; off+=chunk){
    task_t t={0};
    uint64_t len = (off+chunk<=fsize)? chunk:(fsize-off);
    t.kind = TASK_LARGE_STRIPED_CHUNK;
    snprintf(t.path,sizeof(t.path), "%s", path);
    t.size=len; t.offset=off;
    t.layout=L;
    int dom = ost_for_offset(&L, cfg->num_ost, off);
    t.layout.dominant_ost = dom;
    t.flags = 1; // CHUNK_ALIGN_TO_STRIPE
    t.md_ops_hint=1;
    ssend_task_to_owner(&t, cfg->rp, cfg->num_ost);
  }
}

// 递归遍历（基于哈希划分子树到不同Producer，避免重复）
static void traverse_dir(const char* dir, const prod_cfg_t* cfg){
  DIR* dp = opendir(dir);
  if (!dp) return;
  struct dirent* de;
  char path[PATH_MAX];
  while ((de=readdir(dp))){
    if (!strcmp(de->d_name,".") || !strcmp(de->d_name,"..")) continue;
    snprintf(path,sizeof(path), "%s/%s",dir,de->d_name);
    struct stat st;
    if (lstat(path,&st)!=0) continue;
    if (S_ISDIR(st.st_mode)){
      if (assign_dir_to_producer(path, cfg->numP, cfg->myPIndex)){
        traverse_dir(path,cfg);
      }
    }else if (S_ISREG(st.st_mode)){
      uint64_t fsz = (uint64_t)st.st_size;
      layout_t L = fake_layout_for(path, fsz, cfg->num_ost, cfg->stripe_sz, cfg->stripe_cnt);
      if (fsz <= (uint64_t)cfg->small_thresh){
        // 小文件：dominant= start_ost
        emit_small_file_task(path, fsz, L, cfg);
      }else{
        // 大文件：按条带对齐切片，分OST并行
        emit_large_file_chunks(path, fsz, L, cfg);
      }
    }
  }
  closedir(dp);
}

static void producer_main(const prod_cfg_t* cfg){
  // 根目录下第一层按哈希分配
  if (assign_dir_to_producer(cfg->root, cfg->numP, cfg->myPIndex)){
    traverse_dir(cfg->root, cfg);
  }else{
    // 根不属于我：枚举根的子目录并分配
    DIR* dp = opendir(cfg->root);
    if (dp){
      struct dirent* de;
      char path[PATH_MAX];
      while ((de=readdir(dp))){
        if (!strcmp(de->d_name,".") || !strcmp(de->d_name,"..")) continue;
        snprintf(path,sizeof(path), "%s/%s",cfg->root,de->d_name);
        struct stat st;
        if (lstat(path,&st)!=0) continue;
        if (S_ISDIR(st.st_mode)){
          if (assign_dir_to_producer(path, cfg->numP, cfg->myPIndex)){
            traverse_dir(path,cfg);
          }
        }else if (S_ISREG(st.st_mode)){
          // 根下散落的文件归属到某个producer
          if (assign_dir_to_producer(path, cfg->numP, cfg->myPIndex)){
            uint64_t fsz = (uint64_t)st.st_size;
            layout_t L = fake_layout_for(path, fsz, cfg->num_ost, cfg->stripe_sz, cfg->stripe_cnt);
            if (fsz <= (uint64_t)cfg->small_thresh) emit_small_file_task(path, fsz, L, cfg);
            else emit_large_file_chunks(path, fsz, L, cfg);
          }
        }
      }
      closedir(dp);
    }
  }
  // 所有遍历/投递完成，通知所有队列Owner“生产者完成”
  for (int ost=0; ost<cfg->num_ost; ++ost){
    int dst = ost_owner_rank(ost, cfg->rp, cfg->num_ost);
    MPI_Ssend(NULL, 0, MPI_BYTE, dst, TAG_FIN_PROD, MPI_COMM_WORLD);
  }
}

// ---------- 队列Owner：管理per-OST队列，处理TASK_PUT与GET请求，负责反压（队列满时让P阻塞） ----------
typedef struct {
  int managed_osts;       // 本Owner管理的OST数量
  int* ost_ids;           // OST编号列表
  ringq_t* queues;        // 与ost_ids对应的一组队列
  int producers_finished; // 收到的FIN_PROD计数
  int producers_total;    // 全部Producer数量
  int consumers_total;    // 全部Consumer数量
} owner_ctx_t;

static int find_ost_index(const owner_ctx_t* oc, int ost_id){
  for (int i=0;i<oc->managed_osts;i++) if (oc->ost_ids[i]==ost_id) return i;
  return -1;
}

static void owner_main(role_plan_t rp, int rank, int num_ost, int ring_cap){
  // 计算本Owner管理的OST集合（轮询映射）
  int myIndex = rank - rp.baseQ;
  int count=0; for (int o=0;o<num_ost;o++) if (o % rp.numQ == myIndex) count++;
  owner_ctx_t oc={0}; oc.managed_osts=count;
  oc.ost_ids=(int*)malloc(sizeof(int)*count);
  oc.queues =(ringq_t*)malloc(sizeof(ringq_t)*count);
  int k=0; for (int o=0;o<num_ost;o++) if (o % rp.numQ == myIndex){ oc.ost_ids[k]=o; rq_init(&oc.queues[k], ring_cap); k++; }
  oc.producers_total = rp.numP;
  oc.consumers_total = rp.numC;

  int done_broadcasted=0;

  for(;;){
    MPI_Status st; int flag=0;
    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &st);
    if (!flag){
      // 空闲时可小睡，避免忙等
      struct timespec ts={0, 1000000}; nanosleep(&ts,NULL);
      // 若所有Producer已完成且所有队列清空，向所有Consumer广播DONE并退出
      if (!done_broadcasted && oc.producers_finished==oc.producers_total){
        int empty=1; for(int i=0;i<oc.managed_osts;i++) if(!rq_empty(&oc.queues[i])){ empty=0; break; }
        if (empty){
          for (int c=0;c<rp.numC;c++){
            int dst = rp.baseC + c;
            MPI_Send(NULL,0,MPI_BYTE,dst,TAG_DONE,MPI_COMM_WORLD);
          }
          done_broadcasted=1;
          // 等待一小段时间让消费者处理DONE
          struct timespec ts2={0, 5*1000000}; nanosleep(&ts2,NULL);
          break;
        }
      }
      continue;
    }

    if (st.MPI_TAG==TAG_TASK_PUT){
      task_t t; MPI_Recv(&t,sizeof(task_t),MPI_BYTE,st.MPI_SOURCE,TAG_TASK_PUT,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
      int idx = find_ost_index(&oc, t.layout.dominant_ost);
      if (idx<0){ /* 非本Owner管理，忽略（不应发生） */ continue; }
      // 若队列满，忙等直到有空位（对应生产者Ssend阻塞实现反压）
      while (!rq_push(&oc.queues[idx], &t)){
        struct timespec ts={0, 1000000}; nanosleep(&ts,NULL);
      }
    }else if (st.MPI_TAG==TAG_GET_REQ){
      int req[2]; // req[0]=ost_id, req[1]=want(目前固定1)
      MPI_Recv(req,2,MPI_INT,st.MPI_SOURCE,TAG_GET_REQ,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
      int idx = find_ost_index(&oc, req[0]);
      if (idx<0){
        // 不属于我管理，返回空
        int zero=0; MPI_Send(&zero,1,MPI_INT,st.MPI_SOURCE,TAG_GET_RESP,MPI_COMM_WORLD);
      }else{
        task_t t;
        if (rq_pop(&oc.queues[idx], &t)){
          int one=1; MPI_Send(&one,1,MPI_INT,st.MPI_SOURCE,TAG_GET_RESP,MPI_COMM_WORLD);
          MPI_Send(&t,sizeof(task_t),MPI_BYTE,st.MPI_SOURCE,TAG_GET_RESP,MPI_COMM_WORLD);
        }else{
          int zero=0; MPI_Send(&zero,1,MPI_INT,st.MPI_SOURCE,TAG_GET_RESP,MPI_COMM_WORLD);
        }
      }
    }else if (st.MPI_TAG==TAG_FIN_PROD){
      MPI_Recv(NULL,0,MPI_BYTE,st.MPI_SOURCE,TAG_FIN_PROD,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
      oc.producers_finished++;
    }else if (st.MPI_TAG==TAG_DONE){
      // 不期望收到
      MPI_Recv(NULL,0,MPI_BYTE,st.MPI_SOURCE,TAG_DONE,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    }
  }

  for (int i=0;i<oc.managed_osts;i++) rq_free(&oc.queues[i]);
  free(oc.queues); free(oc.ost_ids);
}

// ---------- Consumer：按主队列（亲和）拉取，空则轮询其他OST（简易窃取） ----------
typedef struct {
  int me;
  int num_ost;
  int primary_ost;     // 主队列OST
  role_plan_t rp;
  int simulate_ms_per_mb; // 拷贝时间模拟（毫秒/MB），原型中仅sleep模拟
} cons_cfg_t;

static int req_one_task(int ost, const cons_cfg_t* cfg, task_t* out){
  int owner = ost_owner_rank(ost, cfg->rp, cfg->num_ost);
  int req[2]={ost,1};
  MPI_Send(req,2,MPI_INT,owner,TAG_GET_REQ,MPI_COMM_WORLD);
  int n=0; MPI_Recv(&n,1,MPI_INT,owner,TAG_GET_RESP,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
  if (n==1){
    MPI_Recv(out,sizeof(task_t),MPI_BYTE,owner,TAG_GET_RESP,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    return 1;
  }
  return 0;
}

static void simulate_copy(const task_t* t, const cons_cfg_t* cfg){
  // 原型：仅sleep模拟；真实实现应执行读写（可并行I/O）
  double mb = (double)t->size / (1024.0*1024.0);
  int ms = (int)(mb * cfg->simulate_ms_per_mb);
  if (ms<1) ms=1;
  struct timespec ts={ ms/1000, (ms%1000)*1000000 };
  nanosleep(&ts,NULL);
}

static void consumer_main(const cons_cfg_t* cfg){
  // 主队列优先，空则轮询其他OST（简易窃取）
  int cur = cfg->primary_ost;
  for(;;){
    // DONE检查（异步探测）
    int flag=0; MPI_Status st;
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_DONE, MPI_COMM_WORLD, &flag, &st);
    if (flag){
      MPI_Recv(NULL,0,MPI_BYTE,st.MPI_SOURCE,TAG_DONE,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
      break;
    }
    task_t t;
    if (req_one_task(cur, cfg, &t)){
      simulate_copy(&t,cfg);
      // 黏性：处理完，下一次仍优先同一OST
      continue;
    }else{
      // 轮询下一个OST
      cur = (cur+1) % cfg->num_ost;
      // 避免忙等
      struct timespec ts={0, 500000}; nanosleep(&ts,NULL);
    }
  }
}

// ---------- 主程序 ----------
int main(int argc, char** argv){
  /* MPI 初始化 */
  MPI_Init(&argc,&argv);
  /* 获取进程信息 */
  int rank, world; 
  MPI_Comm_rank(MPI_COMM_WORLD,&rank); 
  MPI_Comm_size(MPI_COMM_WORLD,&world);// 获取进程总数

  /* 参数检查 */
  /* 示例命令： mpirun -n 12 ./pfs_migrate /path/to/root 8 */
  /* 参数说明：源路径 OST个数*/
  if (argc<3){
    if (rank==0) fprintf(stderr,"Usage: %s <root_dir> <num_ost>\n", argv[0]);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  /* 获取根目录以及OST个数 */
  const char* root = argv[1];
  int num_ost = atoi(argv[2]); 
  if (num_ost<=0){// 如果 num_ost<=0 则报错退出
    if(rank==0)fprintf(stderr,"num_ost must be >0\n"); 
    MPI_Abort(MPI_COMM_WORLD,2); 
  }

  /* 根据进程数以及OST个数进行角色划分 */
  role_plan_t rp = plan_roles(world, num_ost);

  /* 公共参数（可用环境变量微调） */
  /* TODO: 把这些参数放到配置文件中 */
  int ring_cap     = env_int("PFS_RING_CAP", 20000);// 每OST队列容量（任务个数）
  int small_thresh = env_int("PFS_SMALL_THRESH", 128*1024);// 小文件阈值128KB（判定为小文件的条件）
  int stripe_sz    = env_int("PFS_STRIPE_SIZE", 4*1024*1024);// 数据条带大小（4MB）
  int stripe_cnt   = env_int("PFS_STRIPE_CNT", 4);// 数据条带数（4条）
  int chunk_mb     = env_int("PFS_CHUNK_MB", 128);// 数据块大小（128MB）
  int sim_ms_per_mb= env_int("PFS_SIM_MS_PER_MB", 1); // 模拟每MB耗时ms

  /* 任务开始前进行信息打印 */
  if (rank==0){
    fprintf(stdout,"[INFO] world=%d, roles: P=%d Q=%d C=%d (bases P:%d Q:%d C:%d), num_ost=%d\n",
      world, rp.numP, rp.numQ, rp.numC, rp.baseP, rp.baseQ, rp.baseC, num_ost);
    fflush(stdout);
  }

  /* 先判断自己的角色 */
  if (is_producer(rank,rp)){
    // 计算我是第几个Producer
    int myPIndex = rank - rp.baseP;
    prod_cfg_t cfg = {
      .root=root, .me=rank, .numP=rp.numP, .myPIndex=myPIndex, .num_ost=num_ost,
      .small_thresh=small_thresh, .stripe_sz=stripe_sz, .stripe_cnt=stripe_cnt, .chunk_mb=chunk_mb, .rp=rp
    };
    producer_main(&cfg);
  }else if (is_qowner(rank,rp)){
    owner_main(rp, rank, num_ost, ring_cap);
  }else if (is_consumer(rank,rp)){
    // 计算我是第几个Consumer，并绑定主队列OST（亲和）
    int myCIndex = rank - rp.baseC;
    cons_cfg_t cfg = { .me=rank, .num_ost=num_ost, .primary_ost = myCIndex % num_ost, .rp=rp, .simulate_ms_per_mb=sim_ms_per_mb };
    consumer_main(&cfg);
  }

  /* 全局通讯域同步 */
  MPI_Barrier(MPI_COMM_WORLD);
  /* 释放资源 */
  MPI_Finalize();
  return 0;
}