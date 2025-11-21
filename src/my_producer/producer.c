/* Producer 通用接口定义 */
#include "producer.h"

prod_cfg_t prod_cfg;// 全局生产者配置变量
/* 生产者配置初始化 */
static void prod_cfg_init(prod_cfg_t* cfg){
  /* 为每个Source_OST 分配一个批处理缓冲区 */
  task_batch_t* batches = (task_batch_t*)calloc(config_env.NUM_SOURCE_OST, sizeof(task_batch_t));
  if (batches == NULL) {
    MFU_LOG(MFU_LOG_ERR, "Failed to allocate memory for task batches.");
    return;
  }
  return ;
}

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
static void emit_large_file_chunks(const char* path, uint64_t fsize, layout_t L){
// ... (之前的函数保持不变) ...

/* 
 * (重写) 生成大文件分片任务，严格按照Lustre条带布局进行切分和批量发送 
 */
static void emit_large_file_chunks(const char* path, uint64_t fsize, mfu_file_layout_t* L, const prod_cfg_t* cfg) {
  /* 关键参数检查 */
  if (L == NULL || L->stripe_size <= 0 || L->stripe_count <= 0) { // 检查文件布局信息是否有效 
    MFU_LOG(MFU_LOG_ERR, "Invalid Lustre layout for file '%s'. Cannot generate chunks.", path); // 记录无效布局的错误日志 
    return; // 如果布局无效，则直接返回，不处理此文件 
  }
  uint64_t stripe_size = L->stripe_size; // 获取文件的条带大小 
  int stripe_count = L->stripe_count; // 获取文件的条带数量（即OST数量） 
  /* 遍历文件中的每一个条带 */
    for (uint64_t stripe_index = 0; ; ++stripe_index) { // 无限循环，直到文件末尾 // 注释
        // --- (3) 计算当前条带在哪个OST上 (轮询布局) ---
        int ost_index_on_file = stripe_index % stripe_count; // 使用取模运算实现轮询 // 注释
        int ost_id = L->ost_ids[ost_index_on_file]; // 从布局信息中获取该OST的真实ID // 注释

        // --- (5) 计算当前条带在文件中的起始偏移量 ---
        uint64_t current_offset = stripe_index * stripe_size; // 当前条带的起始偏移量 // 注释

        // --- (5) 边界条件：检查是否已超出文件大小 ---
        if (current_offset >= fsize) { // 如果计算出的偏移量已经等于或超过文件大小 // 注释
            break; // 说明所有条带都已处理完毕，跳出循环 // 注释
        }

        // --- (5) 边界条件：计算当前条带的实际大小 ---
        uint64_t current_size = stripe_size; // 默认情况下，条带大小就是配置的stripe_size // 注释
        if (current_offset + stripe_size > fsize) { // 如果这是文件的最后一个、不完整的条带 // 注释
            current_size = fsize - current_offset; // 则其实际大小是文件剩余的部分 // 注释
        }

        // --- 创建一个代表此条带的任务 ---
        task_t t = {0}; // 初始化任务结构体为零 // 注释
        t.kind = TASK_LARGE_STRIPED_CHUNK; // 标记任务类型为大文件条带块 // 注释
        snprintf(t.path, sizeof(t.path), "%s", path); // 复制文件路径到任务中 // 注释
        t.size = current_size; // 设置任务的大小为当前条带的实际大小 // 注释
        t.offset = current_offset; // 设置任务的偏移量为当前条带的起始偏移量 // 注释
        t.layout = *L; // 复制整个文件布局信息到任务中 // 注释
        t.layout.dominant_ost = ost_id; // (4) 明确指定此任务归属于哪个OST // 注释

        // --- (1) 将任务放入对应OST所有者的批处理缓冲区 ---
        int rank_dst = ost_owner_rank(&cfg->rp, ost_id); // 计算负责此OST的队列所有者的rank // 注释
        task_batch_t* batch = &batches[rank_dst]; // 获取对应rank的批处理缓冲区指针 // 注释

        batch->tasks[batch->count] = t; // 将新创建的任务复制到缓冲区中 // 注释
        batch->count++; // 增加该缓冲区的任务计数 // 注释

        // --- 检查缓冲区是否已满，如果满了就发送 ---
        if (batch->count == MAX_TASKS_PER_BATCH) { // 如果缓冲区中的任务数达到了批次上限 // 注释
            ssend_task_batch_to_owner(batch, rank_dst); // 调用批量发送函数发送这一整批任务 // 注释
        }
    }

    // --- 收尾工作：发送所有剩余的、未满的批次 ---
    for (int i = 0; i < world_size; i++) { // 遍历所有可能的rank // 注释
        if (batches[i].count > 0) { // 如果某个rank的缓冲区中还有未发送的任务 // 注释
            ssend_task_batch_to_owner(&batches[i], i); // 将这个剩余的批次发送出去 // 注释
        }
    }

    free(batches); // 释放为批处理缓冲区动态分配的内存 // 注释
}
..
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
      
  }else{// 如果该路径是文件。大文件进行切片,小文件聚合
    /* 获取文件布局信息（额外的文件信息） */
    mfu_file_layout_t* layout_current;
    mfu_file_get_layout(path,layout_current);
    /* 如果文件大小大于条带大小则视为大文件 */
    if(st.st_size > layout_current->stripe_size){//大文件进行分片
      emit_large_file_chunks(path, st.st_size, layout_current);
    }else{// 如果是小文件
      emit_small_file_task(path, st.st_size, layout_current);
    } 
  }
  return;
}
/* Producer 主函数 */
static void producer_main(role_plan_t* rp){
    /* 初始化配置 */
    prod_cfg_init(&prod_cfg);
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

    /* 释放为批处理缓冲区分配的内存 */
    free(prod_cfg.batches);
    return ;
}