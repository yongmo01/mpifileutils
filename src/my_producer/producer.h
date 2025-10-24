/* filepath: producer.h */
/* Producer 通用结构与通用接口声明 */
#ifndef _PRODUCER_H
#define _PRODUCER_H
#include "my_common.h"

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

#endif 
