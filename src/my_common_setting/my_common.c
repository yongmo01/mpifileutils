#include "my_common.h"

/*----配置文件相关接口-------*/
config_env_t config_env;//
/* 辅助函数：去除字符串首尾的空白字符，并放回修改后的字符串头尾指针 */
static char* trim_whitespace(char* str) {
  /* 如果str为空指针，返回NULL */
  if(str == NULL) return NULL;
  if(strlen(str) == 0) return str;
  while (isspace((unsigned char)*str)) str++;// 从头部开始去除空白
  if (*str == 0) return str;// 如果全是空白，返回空字符串
  char* end;
  end = str + strlen(str) - 1;// 计算尾部位置
  while (end > str && isspace((unsigned char)*end)) end--;// 从尾部开始去除空白
  end[1] = '\0';// 通过添加字符串结束符来截断字符串，但不是真正的截断字符
  return str;
}
/* 辅助函数：将字符串键映射到枚举值 */
static config_key_t map_key_to_enum(const char* key) {
    if (strcmp(key, "NUM_P") == 0) return KEY_NUM_P;
    if (strcmp(key, "NUM_Q") == 0) return KEY_NUM_Q;
    if (strcmp(key, "NUM_C") == 0) return KEY_NUM_C;
    if (strcmp(key, "SOURCE_PATH") == 0) return KEY_SOURCE_PATH;
    if (strcmp(key, "TARGET_PATH") == 0) return KEY_TARGET_PATH;
    if (strcmp(key, "NUM_SOURCE_MDT") == 0) return KEY_NUM_SOURCE_MDT;
    if (strcmp(key, "NUM_SOURCE_OST") == 0) return KEY_NUM_SOURCE_OST;
    if (strcmp(key, "NUM_TARGET_MDT") == 0) return KEY_NUM_TARGET_MDT;
    if (strcmp(key, "NUM_TARGET_OST") == 0) return KEY_NUM_TARGET_OST;
    if (strcmp(key, "CAP_RING") == 0) return KEY_CAP_RING;
    if (strcmp(key, "TIME_WRITE") == 0) return KEY_TIME_WRITE;
    if (strcmp(key, "TIME_READ") == 0) return KEY_TIME_READ;
    return KEY_UNKNOWN;
}
/* 辅助函数：解析一行 " key = value # comment" 格式的配置行 */
static void parse_config_line(config_env_t* config, char* line) {
  char* key, * value, * separator;// 分离键和值的指针
  /* 忽略注释行和空行 */
  if (line[0] == '#' || line[0] == '\0') {
    return;
  }

  /* 分离 key 和 value */
  separator = strchr(line, '=');// 查找第一次出现 “=” 的位置
  if (separator == NULL) {
    return; // 无效行
  }
  *separator = '\0'; // 将等号替换为字符串结束符，从而分离键和值
  key = trim_whitespace(line);// 去除键的空白 
  value = trim_whitespace(separator + 1);// 去除值的空白
  if (*key == '\0' || *value == '\0') {
    return; // 忽略键或值为空的行
  }

  /* 去除 value 的注释 */
  char* comment = strchr(value, '#');
  if (comment != NULL) {
    *comment = '\0';
  }

 switch (map_key_to_enum(key)) {
  /* --- 数值类型 --- */
  case KEY_NUM_P:
    config->NUM_P = atoi(value);
    break;
  case KEY_NUM_Q:
    config->NUM_Q = atoi(value);
    break;
  case KEY_NUM_C:
    config->NUM_C = atoi(value);
    break;
  case KEY_NUM_SOURCE_MDT:
    config->NUM_SOURCE_MDT = atoi(value);
    break;
  case KEY_NUM_SOURCE_OST:
    config->NUM_SOURCE_OST = atoi(value);
    break;
  case KEY_NUM_TARGET_MDT:
    config->NUM_TARGET_MDT = atoi(value);
    break;
  case KEY_NUM_TARGET_OST:
    config->NUM_TARGET_OST = atoi(value);
    break;
  case KEY_CAP_RING:
    config->CAP_RING = atoi(value);
    break;
  case KEY_STRIPES_PER_TASK:
    config->STRIPES_PER_TASK = atoi(value);
    break;
  case KEY_MAX_TASKS_PER_BATCH:
    config->MAX_TASKS_PER_BATCH = atoi(value);
    break;
  case KEY_TIME_WRITE:
    config->TIME_WRITE = atoi(value);
    break;
  case KEY_TIME_READ:
    config->TIME_READ = atoi(value);
    break;

  /* --- 字符串类型 --- */
  case KEY_SOURCE_PATH:
    strncpy(config->PATH_SOURCE, value, MAX_PATH_LEN - 1);
    config->PATH_SOURCE[MAX_PATH_LEN - 1] = '\0'; // 确保空字符结尾
    break;
  case KEY_TARGET_PATH:
    strncpy(config->PATH_TARGET, value, MAX_PATH_LEN - 1);
    config->PATH_TARGET[MAX_PATH_LEN - 1] = '\0'; // 确保空字符结尾
    break;
  }
}
/* 从指定的配置文件路径加载配置 */
status_config_file_t load_config(config_env_t* config, const char* filepath_config) {
  /* 打开配置文件 */
  FILE* file = fopen(filepath_config, "r");
  if (file == NULL) {// 文件不存在，使用默认值
    fprintf(stderr, "Warning: Config file '%s' not found. Using default values.\n", filepath_config);
    return CONFIG_ERROR_FILE_NOT_FOUND;
  }
  /* 逐行读取配置 */
  char line[MAX_LEN_PATH];
  while (fgets(line, sizeof(line), file)) {
    parse_config_line(config, line);
  }
  /* 关闭配置文件 */
  fclose(file);
  return CONFIG_SUCCESS;
}
/* 计算 源集群 ost -> 队列所有者rank（多OST映射到少量Q） */
bool ost_owner_rank(config_env_t* config){
  /* 对每一个ost 计算该ost归哪个队列所有者 */
  /* 采用轮询的方式分配 */
  for(int i=0;i<config->NUM_SOURCE_OST;i++){
    int idx = i % config->NUM_Q;
    config->MAP_SOURCE_OST[i] = config->NUM_P + idx;
  }
  return true;
}
/* 广播配置到所有进程（由rank 0 发起） */
void broadcast_config(config_env_t* config) {
  
  MPI_Bcast(config, sizeof(config_env_t), MPI_BYTE, 0, MPI_COMM_WORLD);
}
/* 打印当前配置内容（供调试使用） */
void perint_config(const config_env_t* config) {
  printf("Current Configuration:\n");
  printf("NUM_P: %d\n", config->num_p);
  printf("NUM_Q: %d\n", config->num_q);
  printf("NUM_C: %d\n", config->num_c);
  printf("SOURCE_PATH: %s\n", config->source_path);
  printf("TARGET_PATH: %s\n", config->target_path);
  printf("NUM_SOURCE_MDT: %d\n", config->num_source_mdt);
  printf("NUM_SOURCE_OST: %d\n", config->num_source_ost);
  printf("NUM_TARGET_MDT: %d\n", config->num_target_mdt);
  printf("NUM_TARGET_OST: %d\n", config->num_target_ost);
  printf("CAP_RING: %d\n", config->cap_ring);
  printf("TIME_WRITE: %d ms/MB\n", config->time_write);
  printf("TIME_READ: %d ms/MB\n", config->time_read);
}


/*--------环形队列 ringq_t 相关接口--------*/
/* 根据容量初始化队列 */
status_rq_t rq_init(ringq_t* q, int cap) {
  q->buf = (task_t*)malloc(sizeof(task_t)*cap);
  q->capacity = cap; q->head=0; q->tail=0; q->size=0;
  return RQ_SUCCESS;
}
/* 释放队列资源 */
status_rq_t rq_free(ringq_t* q){ 
  free(q->buf); q->buf=NULL; 
  return RQ_SUCCESS;
}    
/* 判断队列是否已满 */
bool rq_full(ringq_t* q){ 
    return q->size==q->capacity; 
}
/* 判断队列是否为空 */
bool rq_empty(ringq_t* q){ 
    return q->size==0; 
}
/* 入队：成功返回 RQ_SUCCESS，失败返回相应状态码 */
status_rq_t rq_push(ringq_t* q, const task_t* t){
  if (rq_full(q)) return RQ_ERROR_FULL;
  q->buf[q->tail]=*t; q->tail=(q->tail+1)%q->capacity; q->size++; return RQ_SUCCESS;
}
/* 出队：成功返回 RQ_SUCCESS，失败返回相应状态码 */
status_rq_t rq_pop(ringq_t* q, task_t* t){
  if (rq_empty(q)) return RQ_ERROR_EMPTY;
  *t = q->buf[q->head]; q->head=(q->head+1)%q->capacity; q->size--; return RQ_SUCCESS;
}


/*-----进程角色分配与计算接口-------*/
/*如果没有通过配置文件指定角色，则使用默认策略*/
bool plan_roles(const config_env_t* config, role_plan_t* rp,int rank,int world){
  // 默认策略：Q = min(num_ost, max(1, world/8)); P = max(1, (world - Q)/4); C = world - P - Q
  // 可通过配置文件覆盖：NUM_P / NUM_Q / NUM_C
  int envP=config->NUM_P, envQ=config->NUM_Q, envC=config->NUM_C;
  if (envP>0 && envQ>0 && envC>0 && envP+envQ+envC==world){// 如果配置文件由指定角色分配
    rp->numP=envP; rp->numQ=envQ; rp->numC=envC;
  }else{
    rp->numQ = rp->num_ost<world? rp->num_ost : (world>8? world/8:1);
    if (rp->numQ<1) rp->numQ=1; 
    if (rp->numQ>world-2) rp->numQ=world-2;
    rp->numP = (world - rp->numQ)/4; 
    if (rp->numP<1) rp->numP=1;
    rp->numC = world - rp->numP - rp->numQ;
    if (rp->numC<1){
      rp->numC=1;
      if(rp->numP>1) rp->numP--;
    }
  }
  rp->baseP=0; rp->baseQ=rp->baseP+rp->numP; rp->baseC=rp->baseQ+rp->numQ;
  /* 设置当前进程角色 */
  if (rank>=rp->baseP && rank<rp->baseP+rp->numP){
    rp->my_role = PRODUCER;
  }else if (rank>=rp->baseQ && rank<rp->baseQ+rp->numQ){
    rp->my_role = QUEUE_OWNER;
  }else{
    rp->my_role = CONSUMER;
  }

  return true;
}
/* 打印角色分配情况 */
void print_role_plan(const role_plan_t* rp){
  printf("Role Plan:\n");
  printf("  Producers: %d (ranks %d to %d)\n", rp->numP, rp->baseP, rp->baseP + rp->numP - 1);
  printf("  Queue Owners: %d (ranks %d to %d)\n", rp->numQ, rp->baseQ, rp->baseQ + rp->numQ - 1);
  printf("  Consumers: %d (ranks %d to %d)\n", rp->numC, rp->baseC, rp->baseC + rp->numC - 1);
}




