/* 通用定义与声明（自己写的 以及 ） */
#ifndef _MY_COMMON_H
#define _MY_COMMON_H


/* 引用 mpiFileUtils 的头文件 */

/* 文件布局头文件 */
#include "layout_aware.h"


/* 额外引用的系统头文件  */


/* 宏定义 */
#define MAX_OST 512  // 假设最大支持512个OST

/*----配置文件相关接口-------*/
static int env_int(const char* k, int dflt);

/*--------任务 task_t 声明-------- */
typedef enum { TASK_SMALL_BATCHABLE=1, TASK_LARGE_STRIPED_CHUNK=2 } task_kind_t;
typedef struct {
  task_kind_t kind;
  char  path[1024];                  // 路径（简化：限制长度）
  uint64_t size;                     // 小文件：文件大小；分片：chunk大小
  uint64_t offset;                  // 小文件：0；分片：chunk起始偏移
  mfu_file_layout_t layout;         // 条带/主导信息
  char  pack_key[256];               // 小文件聚合键（目录+OST），此原型未在消费者端打包，仅示意
  uint32_t flags;                    // 例如 CHUNK_ALIGN_TO_STRIPE = 1
  uint32_t md_ops_hint;              // 估算元数据操作数
} task_t;


/*--------环形队列（用于存放task_t） ringq_t 声明-------- */
/* 简易环形队列（队列所有者rank内部使用；单线程访问，无锁） */
typedef struct {
  task_t *buf;
  int capacity;
  int head;   // 出队位置
  int tail;   // 入队位置
  int size;   // 当前元素数
} ringq_t;

/*--------环形队列 ringq_t 相关接口--------*/
/* 根据容量初始化队列 */
static void rq_init(ringq_t* q, int cap); 
/* 释放队列资源 */
static void rq_free(ringq_t* q);
/* 判断队列是否已满 */
static int  rq_full(ringq_t* q);
/* 判断队列是否为空 */
static int  rq_empty(ringq_t* q);
/* 入队：成功返回1，失败返回0 */
static int  rq_push(ringq_t* q, const task_t* t);
/* 出队：成功返回1，失败返回0 */
static int  rq_pop(ringq_t* q, task_t* t);





/*-----进程角色分配与计算接口-------*/
/* 进程角色枚举 */
typedef enum { PRODUCER=1, QUEUE_OWNER=2, CONSUMER=3 } role;
/* Producer ranks（P）; Queue-owner ranks（Q）; Consumer ranks（C）; */
typedef struct {
  /* 全局视角：各角色数量与进程起始位置 */
  int numP, numQ, numC;        // 各角色数量
  int baseP, baseQ, baseC;     // 各角色起始rank（连续区间）
  /* 当前进程的角色 */
  role my_role;
  /* ost的相关信息 */
  int num_ost;              // 源集群OST数量
  int ost_mapping[MAX_OST];  // 记录每个ost对应的队列所有者rank

} role_plan_t;
/* 自适配角色划分（可通过配置文件指定）：给定world_size（总进程数）与num_ost */
/* 如果没有通过配置文件指定角色，则使用默认策略 */
/* 最后根据当前进程rank设置角色 */
/* 最开始采用静态划分 */
static int plan_roles(role_plan_t* rp,int rank,int world);

/* 计算 源集群 ost -> 队列所有者rank（多OST映射到少量Q） */
/* 成功返回 1 */
static int ost_owner_rank(role_plan_t* rp);





#endif