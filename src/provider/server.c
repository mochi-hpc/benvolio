#include <margo.h>
#include <abt-io.h>
#include <ssg.h>
#include <getopt.h>
#ifdef USE_PMIX
#include <pmix.h>
#include <ssg-pmix.h>
#endif
#ifdef USE_MPI
#include <mpi.h>
#include <ssg-mpi.h>
#endif
#include "bv-provider.h"

#include "bv-config.h"
#ifdef USE_DRC
#include <rdmacred.h>
#endif


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

#ifdef USE_PMIX
int establish_credentials_pmix_all(pmix_proc_t proc, ssg_group_config_t *g_conf)
{
    uint32_t drc_credential_id = -1;
#ifdef USE_DRC
    char *drc_pmix_key;

    asprintf(&drc_pmix_key, "ssg-drc-%s", proc.nspace);
    ASSERT(ret > 0, "asprintf");

    /* rank 0 gets the RDMA credential, stores the identifier in the PMIx kv
     * space, and grants access.  Everyone else will obtain the key and proceed
     */
    if (proc.rank == 0)
    {
	p_ret = drc-acquire(&drc_credential_id, DRC_FLAGS_FLEX_CREDENTIAL);
	ASSERT(p_ret == DRC_SUCCESS, "drc_acquire");

	PMIX_VALUE_LOAD(&value, &drc_credential_id, PMIX_UINT32);
	p_ret = PMIx_Put(PMIX_GLOBAL, drc_pmix_key, &value);
	ASSERT(p_ret == PMIX_SUCCESS, "PMIx_Put");

	p_ret = PMIx_Commit();
	ASSERT(p_ret == PMIX_SUCCESS, "PMIx_Commit");
    }
    PMIX_INFO_CREATE(info, 1);
    flag = true;
    PMIX_INFO_LOAD(info, PMIX_COLLECT_DATA, &flag, PMIX_BOOL);
    p_ret = PMIx_Fence(&proc, 1, info, 1);
    PMIX_INFO_FREE(info, 1);
    ASSERT(p_ret == PMIX_SUCCESS, "PMIX_fence");

    PMIX_PROC_LOAD(&tmp_proc, proc.nspace, 0);
    p_ret = PMIx_Get(&tmp_proc, drc_pmix_key, NULL, 0, &val_p);
    ASSERT(p_ret == PMIX_SUCCESS, "PMIx_Get");
    drc_credential_id = val_p->data.uint32;
    PMIX_VALUE_RELEASE(val_p);

    free(drc_pmix_key);
#endif

    g_conf->ssg_credential = (int64_t)drc_credential_id;
    return 0;
}
#endif

#ifdef USE_MPI
int establish_credentials_mpi_all(MPI_Comm comm, ssg_group_config_t *g_conf)
{
    uint32_t drc_credential_id = -1;
#ifdef USE_DRC
    int ret, rank;
    MPI_Comm_rank(comm, &rank);
    if (rank == 0) {
	ret = drc_acquire(&drc_credential_id, DRC_FLAGS_FLEX_CREDENTIAL);
	ASSERT(ret == DRC_SUCCESS, "drc_acquire: %d", ret);
    }
    MPI_Bcast(&drc_credential_id, 1, MPI_UINT32_T, 0, comm);

#endif
    g_conf->ssg_credential = (int64_t)drc_credential_id;
    return 0;
}
#endif

int main(int argc, char **argv)
{
    margo_instance_id mid;
    abt_io_instance_id abtio;
    bv_svc_provider_t bv_id;
    struct hg_init_info hii = HG_INIT_INFO_INITIALIZER;
    char drc_key_str[256] = {0};
    int ret;
    int nprocs;
    int rank;
    ssg_group_id_t gid;
    ssg_group_config_t g_conf = SSG_GROUP_CONFIG_INITIALIZER;
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


#ifdef USE_PMIX
    pmix_proc_t proc;
    pmix_status_t p_ret;
    ret = PMIx_Init(&proc, NULL, 0);
    ASSERT(ret == PMIX_SUCCESS, "PMIx_Init failed (ret = %d)\n", ret);
    rank = proc.rank;
    ret = establish_credentials_pmix_all(proc, &g_conf);
#endif

#ifdef USE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    ret = establish_credentials_mpi_all(MPI_COMM_WORLD, &g_conf);
#endif

#ifdef USE_DRC
    drc_info_handle_t drc_credential_info;
    uint32_t drc_cookie=0;
    /* access credential on all ranks and convert to string for use by mercury */
    ret = drc_access(g_conf.ssg_credential, 0, &drc_credential_info);
    ASSERT(ret == DRC_SUCCESS, "drc_access: %d", ret);
    drc_cookie = drc_get_first_cookie(drc_credential_info);
    sprintf(drc_key_str, "%u", drc_cookie);
    fprintf(stderr, "drc_key_str: %s (%d)\n", drc_key_str, drc_cookie);

    /* rank 0 grants access to the credential, allowing other jobs to use it */
    if(rank == 0)
    {
        ret = drc_grant(g_conf.ssg_credential, drc_get_wlm_id(), DRC_FLAGS_TARGET_WLM);
        ASSERT (ret == DRC_SUCCESS, "drc_grant: %d", ret);
    }
#endif
    hii.na_init_info.auth_key = drc_key_str;


    /* margo rpc thread count:
     * -1 puts all our rpcs in the mercury context
     *  0 is only for clients (processes not expecting incomming rpcs)
     *  1 will leave us one thread for the cache manager but no threads for incomming RPCS
     *  so 2 or more threads (or -1 threads) needed */
    if (nstreams == 1) {
        nstreams = 2;
        printf("Requested streams too small.  Overriding to %d\n", nstreams);
    }
    mid = margo_init_opt(proto, MARGO_SERVER_MODE, &hii, 0, nstreams);
    ASSERT(mid != MARGO_INSTANCE_NULL, "margo_init_opt (mid=%d)", 0);

    margo_enable_remote_shutdown(mid);

    /* set this is "number of backing threads" to whatever is best for
     * the underlying backing store. */
    abtio = abt_io_init(nthreads);
    margo_push_finalize_callback(mid, finalize_abtio, (void*)abtio);

    ret = ssg_init();
    ASSERT(ret == 0, "ssg_init() failed (ret = %d)\n", ret);


    /* SSG group config parameters:
     * - period length
     * - timeout
     * - subgroup member count for SWIMM
     * - any necessary transport credentials */

#ifdef USE_PMIX
    gid = ssg_group_create_pmix(mid, BV_PROVIDER_GROUP_NAME, proc, &g_conf, NULL, NULL);
    ASSERT(gid != SSG_GROUP_ID_INVALID, "ssg_group_create_pmix() failed (ret = %s)","SSG_GROUP_ID_NULL");
#endif

#ifdef USE_MPI
    gid = ssg_group_create_mpi(mid, BV_PROVIDER_GROUP_NAME, MPI_COMM_WORLD, &g_conf, NULL, NULL);
    ASSERT(gid != SSG_GROUP_ID_INVALID, "ssg_group_create_mpi() failed (ret = %s)" , "SSG_GROUP_ID_NULL");
#endif

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
#ifdef USE_PMIX
    PMIx_Finalize(NULL, 0);
#endif

#ifdef USE_MPI
    MPI_Finalize();
#endif
}
