/* filepath: mfu_layout.h */
/* 文件布局信息的通用结构与通用接口（Lustre、Ceph、BeeGFS）声明 */
#ifndef _LAYOUT_AWARE_H
#define _LAYOUT_AWARE_H

#include <stdint.h>
#include <stdbool.h>



/* 定义文件系统类型枚举 */
typedef enum {
    MFU_FS_UNKNOWN = 0,
    MFU_FS_LUSTRE,
    MFU_FS_CEPH,
    MFU_FS_BEEGFS,
    MFU_FS_GENERIC
} mfu_fs_type;


/* 声明全局变量用于标识源路径和目标路径的文件系统类型 */
extern mfu_fs_type mfu_src_fs_type;
extern mfu_fs_type mfu_dst_fs_type;

/* 文件布局信息的通用结构 */
typedef struct {
    /* 通用属性 */
    mfu_fs_type fs_type;            /* 文件系统类型 */
    bool has_layout;                /* 是否成功获取布局信息 */
    
    /* 通用布局属性 */
    uint64_t stripe_size;           /* 条带大小 (bytes) */
    uint32_t stripe_count;          /* 条带数量 */
    
    /* 文件系统特定信息 */
    union {
        /* Lustre 特定布局 */
        struct {
            uint64_t stripe_offset;     /* 起始OST索引 */
            char pool_name[128];        /* 存储池名称 */
            uint64_t *ost_indices;      /* OST索引数组 */
            uint32_t ost_count;         /* 实际使用的OST数量 */
        } lustre;
        
        /* Ceph 特定布局 */
        struct {
            uint64_t object_size;       /* Ceph对象大小 */
            uint32_t object_count;      /* 对象数量 */
            char pool_name[128];        /* 存储池名称 */
            uint32_t pg_num;            /* 归置组数量 */
            char crush_rule[64];        /* CRUSH规则名称 */
        } ceph;
        
        /* BeeGFS 特定布局 */
        struct {
            uint16_t pattern_type;      /* 条带模式 */
            uint32_t chunk_size;        /* 块大小 */
            uint16_t *targets;          /* 目标存储服务器ID */
            uint32_t target_count;      /* 目标数量 */
        } beegfs;
    } fs;
} mfu_file_layout_t;

/*--------mfu_src_fs_type/mfu_dst_fs_type 相关接口--------*/
/* 根据路径检测文件系统类型 */
const char* get_filesystem_type(const char* path);
/* 获取源路径和目标路径的文件系统类型 */
void mfu_file_detect_fs_type(const char *path,int numpaths,mfu_fs_type* fs_types);
/* 返回源路径的文件系统类型 */
mfu_fs_type mfu_file_get_src_fs_type(void);
/* 返回目标路径的文件系统类型 */
mfu_fs_type mfu_file_get_dst_fs_type(void);


/*--------mfu_file_layout_t 相关接口--------*/
/* 初始化布局结构 */
void mfu_file_layout_init(mfu_file_layout_t *layout);

/* 清理布局结构 */
void mfu_file_layout_free(mfu_file_layout_t *layout);

/* 获取文件布局信息 */
int mfu_file_get_layout(const char *path, mfu_file_layout_t *layout);

/* 设置文件布局信息 */
int mfu_file_set_layout(const char *path, const mfu_file_layout_t *layout);

/* 复制文件布局信息 */
int mfu_file_copy_layout(mfu_file_layout_t *dst, const mfu_file_layout_t *src);

/* 打印布局信息到字符串 */
int mfu_file_layout_to_str(const mfu_file_layout_t *layout, char *str, size_t size);



#endif /* _MFU_LAYOUT_H */