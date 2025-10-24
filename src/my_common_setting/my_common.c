#include "my_common.h"

/*----配置文件相关接口-------*/
static int env_int(const char* k, int dflt){
  const char* v=getenv(k); if (!v) return dflt; char* e; long x=strtol(v,&e,10); return (e==v)?dflt:(int)x;
}


/*--------环形队列 ringq_t 相关接口--------*/
/* 根据容量初始化队列 */
static void rq_init(ringq_t* q, int cap) {
  q->buf = (task_t*)malloc(sizeof(task_t)*cap);
  q->capacity = cap; q->head=0; q->tail=0; q->size=0;
}
/* 释放队列资源 */
static void rq_free(ringq_t* q){ 
    free(q->buf); q->buf=NULL; 
}    
/* 判断队列是否已满 */
static int  rq_full(ringq_t* q){ 
    return q->size==q->capacity; 
}
/* 判断队列是否为空 */
static int  rq_empty(ringq_t* q){ 
    return q->size==0; 
}
/* 入队：成功返回1，失败返回0 */
static int  rq_push(ringq_t* q, const task_t* t){
  if (rq_full(q)) return 0;
  q->buf[q->tail]=*t; q->tail=(q->tail+1)%q->capacity; q->size++; return 1;
}
/* 出队：成功返回1，失败返回0 */
static int  rq_pop(ringq_t* q, task_t* t){
  if (rq_empty(q)) return 0;
  *t = q->buf[q->head]; q->head=(q->head+1)%q->capacity; q->size--; return 1;
}


/*-----进程角色分配与计算接口-------*/
/*如果没有通过配置文件指定角色，则使用默认策略*/
static int plan_roles(role_plan_t* rp,int rank,int world){
  // 默认策略：Q = min(num_ost, max(1, world/8)); P = max(1, (world - Q)/4); C = world - P - Q
  // 可通过环境覆盖：PFS_NUM_P / PFS_NUM_Q / PFS_NUM_C
  int envP=env_int("PFS_NUM_P",-1), envQ=env_int("PFS_NUM_Q",-1), envC=env_int("PFS_NUM_C",-1);
  if (envP>0 && envQ>0 && envC>0 && envP+envQ+envC==world){
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
  
  return 1;
}

/* 计算 源集群 ost -> 队列所有者rank（多OST映射到少量Q） */
static int ost_owner_rank(role_plan_t* rp){
  /* 对每一个ost 计算该ost归哪个队列所有者 */
  /* 采用轮询的方式分配 */
  for(int i=0;i<rp->num_ost;i++){
    int idx = i % rp->numQ;
    rp->ost_mapping[i] = rp->baseQ + idx;
  }
  return 1;
}



