
#include "my_common.h"
#include "my_producer.h"

/* */
int main(int argc, char** argv){
    /* 检查参数个数 */
    /* mpirun -n 8 ./new_cp -c ../config/new_cp.conf */
    if( argc < 3){
        fprintf(stderr, "Usage: %s -c <config_file_path>\n", argv[0]);
        return 1;
    }
    /* 解析参数 */
    char* config_file_path = nullptr;
    for( int i = 1; i < argc; ++i){
        if( strcmp( argv[i], "-c") == 0 && i + 1 < argc){
            config_file_path = argv[i + 1];
            break;
        }
    }

    /* 初始化 MPI */
    MPI_Init(&argc, &argv);// 初始化 MPI 环境
    int rank_world, size_world;// 全局通信器的 rank 和 size
    MPI_Comm_rank(MPI_COMM_WORLD, &rank_world);
    MPI_Comm_size(MPI_COMM_WORLD, &size_world);
    /* 初始化配置（rank 0 负责广播） */
    if( rank_world == 0){
        load_config(&config_env, config_file_path);// 加载配置文件
        broadcast_config(&config_env);// 广播配置
    }
    MPI_Barrier(MPI_COMM_WORLD);// 同步所有进程，确保配置已广播完成
    
    /* 计算角色 */
    role_plan_t rp = plan_roles(&config_env, rank_world, size_world);
    MPI_Barrier(MPI_COMM_WORLD);// 同步所有进程，确保各自的角色已分配完成
    /* COSPLAY */

    return 0;
}
