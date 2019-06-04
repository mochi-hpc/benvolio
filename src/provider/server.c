#include <margo.h>
#include <abt-io.h>
#include <ssg.h>
#include <ssg-mpi.h>
#include "mochio-provider.h"


#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

static void finalize_abtio(void* data) {
    abt_io_instance_id abtio = (abt_io_instance_id)data;
    abt_io_finalize(abtio);
}

static void finalized_ssg_group_cb(void* data)
{
    ssg_group_id_t gid = *((ssg_group_id_t*)data);
    ssg_group_destroy(gid);
}

void print_address(margo_instance_id mid)
{
    hg_addr_t my_address;
    margo_addr_self(mid, &my_address);
    char addr_str[128];
    size_t addr_str_size = 128;
    margo_addr_to_string(mid, addr_str, &addr_str_size, my_address);
    margo_addr_free(mid,my_address);
    printf("Server running at address %s\n", addr_str);
}

void service_config_store(char *filename, ssg_group_id_t gid)
{
    ssg_group_id_store(filename, gid);
}
int main(int argc, char **argv)
{
    margo_instance_id mid;
    abt_io_instance_id abtio;
    mochio_svc_provider_t mochio_id;
    int ret;
    int rank;
    ssg_group_id_t gid;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    mid = margo_init(argv[1], MARGO_SERVER_MODE, 0, 1);
    margo_enable_remote_shutdown(mid);

    /* set this is "number of backing threads" to whatever is best for
     * the underlying backing store. : TODO: should make it command line
     * configurable */
    abtio = abt_io_init(2);
    margo_push_finalize_callback(mid, finalize_abtio, (void*)abtio);

    ret = ssg_init(mid);
    ASSERT(ret == 0, "ssg_init() failed (ret = %d)\n", ret);
    gid = ssg_group_create_mpi(MOCHIO_PROVIDER_GROUP_NAME, MPI_COMM_WORLD, NULL, NULL);
    ASSERT(gid != SSG_GROUP_ID_NULL, "ssg_group_create_mpi() failed (ret = %s)","SSG_GROUP_ID_NULL");
    margo_push_finalize_callback(mid, &finalized_ssg_group_cb, (void*)&gid);

    if (rank == 0)
        service_config_store(argv[2], gid);

    ret = mochio_svc_provider_register(mid, abtio, ABT_POOL_NULL, gid, &mochio_id);

    margo_wait_for_finalize(mid);
    margo_finalize(mid);
}
