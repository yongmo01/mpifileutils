/* 通用定义与声明（自己写的 以及 ） */
#ifndef _MY_COMMON_H
#define _MY_COMMON_H


/* 引用 mpiFileUtils 的头文件 */
#include "libcircle.h"
#include "mfu.h"

/* 文件布局头文件 */
#include "layout_aware.h"


/* 额外引用的系统头文件  */
# include <mpi.h>

/* 宏定义 */
#define MAX_NUM_OST 512  // 假设最大支持512个OST
#define MAX_LEN_PATH 4096 // 为字符串路径定义一个最大长度
#define TAG_TASK_PUT 1 // 单个任务的Tag
#define TAG_TASK_BATCH_PUT 2 // 批量任务的Tag

/*----环境配置 env_config_t 声明-------*/
typedef struct {
  /* 进程数与角色分配 */
  uint32_t NUM_TOTAL;
  uint32_t NUM_P;
  uint32_t NUM_Q;
  uint32_t NUM_C;

  /*源集群文件系统配置*/
  uint32_t NUM_SOURCE_MDT;
  uint32_t NUM_SOURCE_OST;

  /*目标集群文件系统配置*/
  uint32_t NUM_TARGET_MDT;
  uint32_t NUM_TARGET_OST;

  /* 路径配置 */
  char PATH_SOURCE[MAX_LEN_PATH];
  char PATH_TARGET[MAX_LEN_PATH];

  /* 环形队列配置 */
  uint32_t CAP_RING;
  int MAP_SOURCE_OST[MAX_NUM_OST];  // 记录每个ost对应的队列所有者rank

  /* 任务与批处理配置 */
  uint32_t STRIPES_PER_TASK;    
  uint32_t MAX_TASKS_PER_BATCH;  
  /* 模拟I/O耗时配置 (单位: 毫秒/MB) */
  uint32_t TIME_WRITE;
  uint32_t TIME_READ;

} config_env_t;
/* 配置文件读取状态枚举 */
typedef enum {
  CONFIG_SUCCESS = 0,// 读取成功
  CONFIG_ERROR_FILE_NOT_FOUND = 1,// 文件不存在
  CONFIG_ERROR_PARSE_FAILED = 2,// 解析错误
} status_config_file_t;
/* 配置键枚举 */
typedef enum {
    KEY_UNKNOWN, // 未知键
    KEY_NUM_P,
    KEY_NUM_Q,
    KEY_NUM_C,
    KEY_SOURCE_PATH,
    KEY_TARGET_PATH,
    KEY_NUM_SOURCE_MDT,
    KEY_NUM_SOURCE_OST,
    KEY_NUM_TARGET_MDT,
    KEY_NUM_TARGET_OST,
    KEY_CAP_RING,
    KEY_TIME_WRITE,
    KEY_TIME_READ
} key_config_t;
extern config_env_t config_env;// 全局配置变量


/*----配置文件相关接口-------*/
/**
 * @brief 从指定的配置文件路径加载配置。
 *
 * 这个函数应该只由 rank 0 进程调用，然后将结果广播给其他进程。
 * 它会读取文件，解析键值对，并填充 config_env_t 结构体。
 *
 * @param config 指向要填充的配置结构体的指针。
 * @param filepath_config 配置文件的路径。
 * @return 成功返回 CONFIG_SUCCESS，文件找不到或解析失败返回相应错误码。
 */
status_config_file_t load_config(config_env_t* config, const char* filepath_config);
/**
 * @brief 计算 源集群 ost -> 队列所有者rank（多OST映射到少量Q）
 * 成功返回 true ;失败返回 false
 * @param config 指向配置结构体的指针。
 */
bool ost_owner_rank(config_env_t* config);
/**
 * @brief 将配置从 rank 0 广播到所有其他进程。
 *
 * @param config 指向配置结构体的指针。在 rank 0 上是输入，在其他rank上是输出。
 */
status_config_file_t broadcast_config(config_env_t* config);
/**
 * @brief 打印当前配置内容（供调试使用）。
 *
 * @param config 指向配置结构体的指针。
 */
void print_config(const config_env_t* config);

/*--------任务 task_t 声明---------*/
typedef enum { TASK_SMALL_BATCHABLE=1, TASK_LARGE_STRIPED_CHUNK=2 } task_kind_t;
typedef struct {
  /* 任务基本信息 */
  char  path[MAX_LEN_PATH];                  // 路径（简化：限制长度）
  uint64_t size;                     // 小文件：文件大小；分片：chunk大小
  uint64_t offset;                  // 起始位置
  uint64_t stripe_size;             // 条带大小（读取的粒度）
  uint32_t stripe_step;             // 条带步长（OST的个数），用于跳着读
  bool is_logically_contiguous;//表示该任务是否涉及文件尾部，即逻辑连续
  /* maybe  */
  
  char  pack_key[256];               // 小文件聚合键（目录+OST），此原型未在消费者端打包，仅示意
} task_t;
/*--------任务批次 task_batch_t 声明-------- */
typedef struct {
    uint32_t count; // 当前批次中的任务数量
    task_t* tasks; // 存储任务的数组
} task_batch_t;

/*--------环形队列（用于存放task_t） ringq_t 声明-------- */
/* 简易环形队列（队列所有者rank内部使用；单线程访问，无锁） */
typedef struct {
  task_t *buf;
  int capacity;
  int head;   // 出队位置
  int tail;   // 入队位置
  int size;   // 当前元素数
} ringq_t;
/* 环形队列状态枚举 */
typedef enum {
    RQ_SUCCESS = 0,
    RQ_ERROR_FULL = 1,
    RQ_ERROR_EMPTY = 2,
    RQ_ERROR_NULL_POINTER = 3,
    RQ_ERROR_ALLOC_FAILED = 4
} status_rq_t;
/*--------环形队列 ringq_t 相关接口--------*/
/* 根据容量初始化队列：成功返回 RQ_SUCCESS ;失败返回 相应状态码 */
status_rq_t rq_init(ringq_t* q, int cap); 
/* 释放队列资源：成功返回 RQ_SUCCESS ;失败返回 相应状态码 */
status_rq_t rq_free(ringq_t* q);
/* 判断队列是否已满：队列已满返回 true ;否则返回 false */
bool rq_full(ringq_t* q);
/* 判断队列是否为空：队列为空返回 true ;否则返回 false */
bool rq_empty(ringq_t* q);
/* 入队：成功返回 RQ_SUCCESS ;失败返回 相应状态码 */
status_rq_t rq_push(ringq_t* q, const task_t* t);
/* 出队：成功返回 RQ_SUCCESS ;失败返回 相应状态码 */
status_rq_t rq_pop(ringq_t* q, task_t* t);



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
} role_plan_t;
/* 自适配角色划分（可通过配置文件指定）：给定world_size（总进程数）与num_ost */
/* 如果没有通过配置文件指定角色，则使用默认策略 */
/* 最后根据当前进程rank设置角色 */
/* 最开始采用静态划分 */
/* 成功返回 true ;失败返回 false */
/*-------TODO:修改返回值----*/
bool plan_roles(role_plan_t* rp,int rank,int world);
/* 打印角色分配情况 */
void print_role_plan(const role_plan_t* rp);






#endif