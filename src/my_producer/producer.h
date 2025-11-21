/* filepath: producer.h */
/* Producer 通用结构与通用接口声明 */
#ifndef _PRODUCER_H
#define _PRODUCER_H
#include "my_common.h"

// ---------- Producer：遍历 + 任务生成 + 同步发送到对应队列Owner（队列满则阻塞=反压） ----------
/* Producer 配置参数 */
typedef struct {
  int me, numP, myPIndex;// me=rank;numP=总Producer数;myPIndex=rank-baseP（逻辑上第几个Producer）
  task_batch_t* batches;// 每个OST对应的批处理缓冲区数组指针
} prod_cfg_t;
extern prod_cfg_t prod_cfg;// 全局生产者配置变量
/* 生产者配置初始化 */
static void prod_cfg_init(prod_cfg_t* cfg);

/* 将task发送到该OST队列所有者 */
static void ssend_task_to_owner(const task_t* t, role_plan_t rp, int num_ost);

/* 生成小文件任务 */
static void emit_small_file_task(const char* path, uint64_t fsize, layout_t L, const prod_cfg_t* cfg);

/* 生成大文件分片任务 */
static void emit_large_file_chunks(const char* path, uint64_t fsize, layout_t L, const prod_cfg_t* cfg);

/* 任务队列初始化，只有 circle_global_rank==0 的进程才会执行*/
static void producer_create(CIRCLE_handle* handle);

/* Producer 主函数 */
static void producer_main(role_plan_t* rp);
#endif 
