#include <margo.h>
#include <abt-io.h>
#include <ssg.h>
#include <ssg-pmix.h>
#include <getopt.h>
#include <pmix.h>
#include "bv-provider.h"


#define ASSERT(__cond, __msg, ...) { if(!(__cond)) { fprintf(stderr, "[%s:%d] " __msg, __FILE__, __LINE__, __VA_ARGS__); exit(-1); } }

static void finalize_abtio(void* data) {
    abt_io_instance_id abtio = (abt_io_instance_id)data;
    abt_io_finalize(abtio);
}

typedef struct {
    ssg_group_id_t g_id;
    margo_instance_id m_id;
} finalize_args_t;
static void finalized_ssg_group_cb(void* data)
{
    finalize_args_t *args = (finalize_args_t *)data;
    ssg_group_destroy(args->g_id);
    ssg_finalize();
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

void service_config_store(char *filename, ssg_group_id_t gid, int count)
{
    ssg_group_id_store(filename, gid, count);
}
int main(int argc, char **argv)
{
    margo_instance_id mid;
    abt_io_instance_id abtio;
    bv_svc_provider_t bv_id;
    pmix_proc_t proc;
    int ret;
    int rank, nprocs;
    ssg_group_id_t gid;
    int c;
    char *proto=NULL;
    char *statefile=NULL;
    int bufsize=1024;
    int xfersize=1024;
    int nthreads=4;
    int nstreams=4;

    while ( (c = getopt(argc, argv, "p:b:s:t:f:x:" )) != -1) {
        switch (c) {
            case 'p':
                proto = strdup(optarg);
                break;
            case 'b':
                bufsize = atoi(optarg);
                break;
            case 's':
                nstreams = atoi(optarg);
                break;
            case 't':
                nthreads = atoi(optarg);
                break;
            case 'f':
                statefile = strdup(optarg);
                break;
            case 'x':
                xfersize = atoi(optarg);
                break;
            default:
                printf("usage: %s [-p address] [-b buffer_size] [-t threads] [-s streams] [-f statefile] [-x xfersize]\n", argv[0]);
                exit(-1);
        }
    }

    mid = margo_init(proto, MARGO_SERVER_MODE, 0, nstreams);
    margo_enable_remote_shutdown(mid);

    /* set this is "number of backing threads" to whatever is best for
     * the underlying backing store. */
    abtio = abt_io_init(nthreads);
    margo_push_finalize_callback(mid, finalize_abtio, (void*)abtio);

    ret = PMIx_Init(&proc, NULL, 0);
    ASSERT(ret == PMIX_SUCCESS, "PMIx_Init failed (ret = %d)\n", ret);

    ret = ssg_init();
    ASSERT(ret == 0, "ssg_init() failed (ret = %d)\n", ret);
    gid = ssg_group_create_pmix(mid, BV_PROVIDER_GROUP_NAME, proc, NULL, NULL, NULL);
    ASSERT(gid != SSG_GROUP_ID_INVALID, "ssg_group_create_pmix() failed (ret = %s)","SSG_GROUP_ID_NULL");
    finalize_args_t args = {
        .g_id = gid,
        .m_id = mid
    };
    margo_push_prefinalize_callback(mid, &finalized_ssg_group_cb, (void*)&args);


    nprocs = ssg_get_group_size(gid);
    /* only one member of the SSG group needs to write out the file.  Clients
     * will load and deserialize the file to find this ssg group and observe it */

    if (ssg_get_group_self_rank(gid) == 0)
        service_config_store(statefile, gid, nprocs);

    ret = bv_svc_provider_register(mid, abtio, ABT_POOL_NULL, gid, bufsize, xfersize, &bv_id);
    free(proto);
    free(statefile);

    margo_wait_for_finalize(mid);
    PMIx_Finalize(NULL, 0);
}
