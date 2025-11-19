/* Producer 通用接口定义 */
#include "producer.h"


/* 将task发送到该OST队列所有者 */
static void ssend_task_to_owner(const task_t* t, role_plan_t rp){
  int rank_dst = config_env.MAP_SOURCE_OST[t->layout.dominant_ost]; 
  // 同步发送：当队列所有者不接收或队列满时，发送阻塞，实现“反压”，避免无限膨胀
  MPI_Ssend((void*)t, sizeof(task_t), MPI_BYTE, rank_dst, TAG_TASK_PUT, MPI_COMM_WORLD);
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
/* 任务队列初始化，只有 circle_global_rank==0 的进程才会执行*/
static void producer_create(CIRCLE_handle* handle){
  // 将源路径放入到任务队列中
  handle->enqueue(config_env.PATH_SOURCE);
}
/* 每个生产者在从队列中获取一个路径的时候都会执行以下函数* /
/* 如果该路径是目录，则遍历该目录的下的所有条目，并将其放回到队列中 */
/* 如果该路径是文件，则将该文件包装成一个任务 */
static void producer_process(CIRCLE_handle* handle){
  /* 从队列中获取待遍历目录/文件 路径 */
  char path[MAX_LEN_PATH];
  handle->dequeue(path);
  mfu_file_t* mfu_file = *CURRENT_PFILE;

  /* 获取该文件/目录 的元数据 */
  struct stat st;
  int status;
  status = mfu_file_lstat(path, &st, mfu_file);//假设不考虑链接，只考虑符号链接本身的信息
  if (status != 0) {//如果获取元数据失败
    MFU_LOG(MFU_LOG_ERR, "Failed to stat: '%s' (errno=%d %s)",
    path, errno, strerror(errno));
    WALK_RESULT = -1;
    return;
  }

  /* increment our item count */
  reduce_items++;

  if (S_ISDIR(st.st_mode)) {// 如果该路径是目录 
      
  }else{// 如果该路径是文件。大文件进行切片

  }
  return;
}
/* Producer 主函数 */
static void producer_main(role_plan_t* rp){
    /* 初始化配置 */
    prod_cfg_t prod_cfg;

    /* 初始化 MPI 环境(全局) */
    int world_rank, world_size;// 全局通信器的 rank 和 size
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    /* 创建只包含 生产者 的通信域 */
    /* 使用 该进程的全局通信域下的rank 保持在新通信域的相对顺序 */
    MPI_Comm comm_producer;
    MPI_Comm_split(MPI_COMM_WORLD, rp->my_role, world_rank, &comm_producer);
    int comm_producer_rank, comm_producer_size;// 生产者通信域的 rank 和 size
    MPI_Comm_rank(comm_producer, &comm_producer_rank);
    MPI_Comm_size(comm_producer, &comm_producer_size);

    /* 初始化circle*/
    int  circle_global_rank;// 记录该生产者在 CIRCLE 内部的 rank
    circle_global_rank = CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_TERM_TREE, comm_producer);
    /* 设置 ciecle 日志详细程度 */
    enum CIRCLE_loglevel circle_loglevel = CIRCLE_LOG_INFO;
    CIRCLE_enable_logging(circle_loglevel);
    /* 注册回调函数 */
    CIRCLE_cb_create(&producer_create);//
    CIRCLE_cb_process(&producer_process);//
    CIRCLE_cb_reduce_init(&reduce_init);// 归约初始化
    CIRCLE_cb_reduce_op(&reduce_exec);// 归约核心执行函数
    CIRCLE_cb_reduce_fini(&reduce_fini);// 归约结束处理函数

    /* 开始运行 circle  */
    CIRCLE_begin();
    CIRCLE_finalize(); 

}