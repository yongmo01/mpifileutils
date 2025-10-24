/* filepath: mfu_layout.c */
#include "layout_aware.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/vfs.h>  // 使用 statfs 而不是 statvfs
#include <mntent.h>   // 用于读取挂载信息


/* 包含文件系统特定的头文件 */ 
/* 只有编译时加入参数如 "-DENABLE_LUSTRE=ON" 才会启动对应代码块 */
#ifdef LUSTRE_SUPPORT
#include <lustre/lustreapi.h>
#include <lustre/lustre_user.h>
#endif

/* 暂时不支持 Ceph 和 BeeGFS 
#ifdef CEPH_SUPPORT
#include <cephfs/libcephfs.h>
#endif

#ifdef BEEGFS_SUPPORT
#include <beegfs/beegfs.h>
#endif
*/


/*--------mfu_src_fs_type/mfu_dst_fs_type 相关接口--------*/
/* 根据路径检测文件系统类型 */
const char* get_filesystem_type(const char* path) {
    // 通过读取 /proc/mounts 或 /etc/mtab 获取文件系统类型
    FILE* mtab = setmntent("/proc/mounts", "r");
    if (mtab == NULL) {
        mtab = setmntent("/etc/mtab", "r");
        if (mtab == NULL) {
            perror("Could not open /proc/mounts or /etc/mtab");
            return NULL;
        }
    }

    struct mntent* entry;
    char* resolved_path = realpath(path, NULL);
    if (!resolved_path) {
        perror("realpath failed");
        endmntent(mtab);
        return NULL;
    }

    const char* fs_type = NULL;
    while ((entry = getmntent(mtab)) != NULL) {
        char* mnt_resolved = realpath(entry->mnt_dir, NULL);
        if (mnt_resolved && strncmp(resolved_path, mnt_resolved, strlen(mnt_resolved)) == 0) {
            if (strlen(mnt_resolved) > strlen(fs_type ? "" : "") && 
                (fs_type == NULL || strlen(mnt_resolved) > strlen(fs_type))) {
                // 使用最长匹配的挂载点
                free((void*)fs_type);
                fs_type = strdup(entry->mnt_type);
            }
        }
        free(mnt_resolved);
    }

    free(resolved_path);
    endmntent(mtab);
    return fs_type;
}
/* 获取源路径和目标路径的文件系统类型 */
void mfu_file_detect_fs_type(const char* path,int numpaths,mfu_fs_type* fs_type)
{
    if(path == NULL || fs_type == NULL || numpaths != 1)
    {
        return ;
    }
    const char* result = get_filesystem_type(path);
    if (result) {
        if (strcmp(result, "lustre") == 0) {
            fs_type = MFU_FS_LUSTRE;
        } else if (strcmp(result, "ceph") == 0) {
            fs_type = MFU_FS_CEPH;
        } else if (strcmp(result, "beegfs") == 0) {
            fs_type = MFU_FS_BEEGFS;
        } else {
            fs_type = MFU_FS_GENERIC;
        }
    }
    return ;
}
/* 返回源路径的文件系统类型 */
mfu_fs_type mfu_file_get_src_fs_type(void)
{
    return mfu_src_fs_type;
}
/* 返回目标路径的文件系统类型 */
mfu_fs_type mfu_file_get_dst_fs_type(void)
{
    return mfu_dst_fs_type;
}


/*--------mfu_file_layout_t 相关接口--------*/
/* 初始化布局结构 */
void mfu_file_layout_init(mfu_file_layout_t *layout)
{
    /* 参数检查 */
    if (layout == NULL) 
    {
        return;
    }
    
    memset(layout, 0, sizeof(mfu_file_layout_t));//初始化内存，填充0
    /* 初始化变量 */
    layout->fs_type = MFU_FS_UNKNOWN;
    layout->has_layout = false;
    layout->stripe_size = 0;
    layout->stripe_count = 0;
}

/* 清理布局结构 */
void mfu_file_layout_free(mfu_file_layout_t *layout)
{
    /* 参数检查 */
    if (layout == NULL) 
    {
        return;
    }

    /* 释放特定文件系统资源 */
    switch (layout->fs_type) 
    {
        case MFU_FS_LUSTRE: // 如果是 Lustre 文件系统
            if (layout->fs.lustre.ost_indices)
            {
                free(layout->fs.lustre.ost_indices);
                layout->fs.lustre.ost_indices = NULL;
            }
            break;
        case MFU_FS_BEEGFS:// 如果是 BeeGFS 文件系统
            if (layout->fs.beegfs.targets) 
            {
                free(layout->fs.beegfs.targets);
                layout->fs.beegfs.targets = NULL;
            }
            break;
        default:
            break;
    }
    /* 重置布局信息 */
    layout->has_layout = false;
}


/* 获取Lustre文件布局的接口 */
#ifdef LUSTRE_SUPPORT 
static int mfu_file_get_lustre_layout(const char *path, mfu_file_layout_t *layout)
{
    struct lov_user_md_v3 *lum;
    int fd, rc;
    int lum_size = sizeof(struct lov_user_md_v3) + 
                   LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
    
    /* 分配内存存储布局信息 */
    lum = (struct lov_user_md_v3 *)malloc(lum_size);
    if (!lum) {
        return -ENOMEM;
    }
    
    /* 打开文件 */
    fd = open(path, O_RDONLY);
    if (fd < 0) {
        free(lum);
        return -errno;
    }
    
    /* 获取布局信息 */
    memset(lum, 0, lum_size);
    rc = ioctl(fd, LL_IOC_LOV_GETSTRIPE, lum);
    close(fd);
    
    if (rc < 0) {
        free(lum);
        return -errno;
    }
    
    /* 填充布局信息 */
    layout->fs_type = MFU_FS_LUSTRE;
    layout->stripe_size = lum->lmm_stripe_size;
    layout->stripe_count = lum->lmm_stripe_count;
    layout->fs.lustre.stripe_offset = lum->lmm_stripe_offset;
    layout->fs.lustre.ost_count = lum->lmm_stripe_count;
    
    /* 获取存储池名称 */
    if (lum->lmm_magic == LOV_USER_MAGIC_V3) {
        strncpy(layout->fs.lustre.pool_name, lum->lmm_pool_name, 
                sizeof(layout->fs.lustre.pool_name) - 1);
    } else {
        layout->fs.lustre.pool_name[0] = '\0';
    }
    
    /* 获取OST索引信息 */
    if (lum->lmm_stripe_count > 0) {
        layout->fs.lustre.ost_indices = (uint64_t*)malloc(
            lum->lmm_stripe_count * sizeof(uint64_t));
        
        if (layout->fs.lustre.ost_indices) {
            struct lov_user_ost_data_v1 *objects = lum->lmm_objects;
            for (uint32_t i = 0; i < lum->lmm_stripe_count; i++) {
                layout->fs.lustre.ost_indices[i] = objects[i].l_ost_idx;
            }
        }
    }
    
    layout->has_layout = true;
    free(lum);
    return 0;
}
#endif

/* 获取Ceph文件布局的接口 */
#ifdef CEPH_SUPPORT
static int mfu_file_get_ceph_layout(const char *path, mfu_file_layout_t *layout)
{
    struct ceph_mount_info *cmount = NULL;
    int rc;
    
    /* 初始化Ceph挂载 */
    rc = ceph_create(&cmount, NULL);
    if (rc != 0) {
        return -rc;
    }
    
    rc = ceph_conf_read_file(cmount, NULL);
    if (rc != 0) {
        ceph_shutdown(cmount);
        return -rc;
    }
    
    rc = ceph_mount(cmount, NULL);
    if (rc != 0) {
        ceph_shutdown(cmount);
        return -rc;
    }
    
    /* 获取文件布局信息 */
    int fd = ceph_open(cmount, path, O_RDONLY, 0);
    if (fd < 0) {
        ceph_shutdown(cmount);
        return fd;
    }
    
    /* 获取布局信息 */
    struct ceph_file_layout l;
    rc = ceph_get_file_layout(cmount, fd, &l);
    ceph_close(cmount, fd);
    
    if (rc != 0) {
        ceph_shutdown(cmount);
        return -rc;
    }
    
    /* 填充布局信息 */
    layout->fs_type = MFU_FS_CEPH;
    layout->stripe_size = l.fl_object_size;
    layout->stripe_count = l.fl_stripe_count;
    layout->fs.ceph.object_size = l.fl_object_size;
    layout->fs.ceph.object_count = l.fl_stripe_count;
    
    /* 获取更多详细信息可能需要额外的Ceph API调用 */
    /* ... */
    
    layout->has_layout = true;
    ceph_shutdown(cmount);
    return 0;
}
#endif

/* 获取BeeGFS文件布局的接口 */
#ifdef BEEGFS_SUPPORT
static int mfu_file_get_beegfs_layout(const char *path, mfu_file_layout_t *layout)
{
    /* BeeGFS API调用示例 */
    /* 注意：具体API可能需要根据BeeGFS版本进行调整 */
    
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return -errno;
    }
    
    /* 假设的BeeGFS API调用 */
    BeegfsIoctl_GetChunkInfo chunkInfo;
    memset(&chunkInfo, 0, sizeof(chunkInfo));
    
    int rc = ioctl(fd, BEEGFS_IOC_GETCHUNKINFO, &chunkInfo);
    close(fd);
    
    if (rc < 0) {
        return -errno;
    }
    
    /* 填充布局信息 */
    layout->fs_type = MFU_FS_BEEGFS;
    layout->stripe_size = chunkInfo.chunkSize;
    layout->stripe_count = chunkInfo.numTargets;
    layout->fs.beegfs.pattern_type = chunkInfo.storagePattern;
    layout->fs.beegfs.chunk_size = chunkInfo.chunkSize;
    
    /* 获取目标信息 */
    if (chunkInfo.numTargets > 0) {
        layout->fs.beegfs.targets = (uint16_t*)malloc(
            chunkInfo.numTargets * sizeof(uint16_t));
        
        if (layout->fs.beegfs.targets) {
            for (uint32_t i = 0; i < chunkInfo.numTargets; i++) {
                layout->fs.beegfs.targets[i] = chunkInfo.targetIDs[i];
            }
            layout->fs.beegfs.target_count = chunkInfo.numTargets;
        }
    }
    
    layout->has_layout = true;
    return 0;
}
#endif

/* 
    获取文件布局信息统一接口
    参数说明: path - 文件路径 ; layout - 指向布局结构的指针
*/
int mfu_file_get_layout(const char *path, mfu_file_layout_t *layout)
{
    if (!path || !layout) 
    {
        return -EINVAL;
    }
    
    /* 清理已有布局信息 */
    mfu_file_layout_free(layout);
    mfu_file_layout_init(layout);
    
    /* 检测文件系统类型*/
    mfu_fs_type fs_type = mfu_file_get_src_fs_type();
    layout->fs_type = fs_type;
    
    /* 根据文件系统类型调用相应的获取布局函数 */
    switch (fs_type) 
    {
        /* Lustre */
        case MFU_FS_LUSTRE:
                #ifdef LUSTRE_SUPPORT
                    return mfu_file_get_lustre_layout(path, layout);
                #else
                    return -ENOTSUP;
                #endif
        /* Ceph */ 
        case MFU_FS_CEPH:
                #ifdef CEPH_SUPPORT
                    return mfu_file_get_ceph_layout(path, layout);
                #else
                    return -ENOTSUP;
                #endif
        /* BeeGFS */   
        case MFU_FS_BEEGFS:
                #ifdef BEEGFS_SUPPORT
                    return mfu_file_get_beegfs_layout(path, layout);
                #else
                    return -ENOTSUP;
                #endif   
        default:
            return -ENOTSUP;
    }
}





