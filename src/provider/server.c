#define _GNU_SOURCE

#include <margo.h>
#include <abt-io.h>
#include <ssg.h>
#include <getopt.h>
#ifdef USE_PMIX
#include <stdio.h>
#include <pmix.h>
#include <ssg-pmix.h>
#endif
#ifdef USE_MPI
#include <mpi.h>
#include <ssg-mpi.h>
#endif
#include "common.h"
#include "bv-provider.h"

#include "bv-config.h"
#ifdef USE_DRC
#include <rdmacred.h>
#endif

#include <json-c/json.h>


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

static char* readfile(const char* filename) {
    FILE *f = fopen(filename, "r");
    int ret;
    if(!f) {
        perror("fopen");
        fprintf(stderr, "\tCould not open json file \"%s\"\n", filename);
        exit(EXIT_FAILURE);
    }
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    char* string = malloc(fsize + 1);
    ret = fread(string, 1, fsize, f);
    if(ret < 0) {
        perror("fread");
        fprintf(stderr, "\tCould not read json file \"%s\"\n", filename);
        exit(EXIT_FAILURE);
    }
    fclose(f);
    string[fsize] = 0;
    return string;
}

#ifdef USE_PMIX
int establish_credentials_pmix_all(pmix_proc_t proc, ssg_group_config_t *g_conf)
{
    uint32_t drc_credential_id = -1;
#ifdef USE_DRC
    char *drc_pmix_key;
    pmix_proc_t tmp_proc;
    pmix_value_t value, *val_p;
    pmix_info_t *info;
    int ret;
    pmix_status_t p_ret;
    bool flag;

    ret = asprintf(&drc_pmix_key, "ssg-drc-%s", proc.nspace);
    ASSERT(ret > 0, "asprintf", ret);

    /* rank 0 gets the RDMA credential, stores the identifier in the PMIx kv
     * space, and grants access.  Everyone else will obtain the key and proceed
     */
    if (proc.rank == 0)
    {
	ret = drc_acquire(&drc_credential_id, DRC_FLAGS_FLEX_CREDENTIAL);
	ASSERT(ret == DRC_SUCCESS, "drc_acquire", ret);

	PMIX_VALUE_LOAD(&value, &drc_credential_id, PMIX_UINT32);
	p_ret = PMIx_Put(PMIX_GLOBAL, drc_pmix_key, &value);
	ASSERT(p_ret == PMIX_SUCCESS, "PMIx_Put", 0);

	p_ret = PMIx_Commit();
	ASSERT(p_ret == PMIX_SUCCESS, "PMIx_Commit", 0);
    }
    PMIX_INFO_CREATE(info, 1);
    flag = true;
    PMIX_INFO_LOAD(info, PMIX_COLLECT_DATA, &flag, PMIX_BOOL);
    p_ret = PMIx_Fence(&proc, 1, info, 1);
    PMIX_INFO_FREE(info, 1);
    ASSERT(p_ret == PMIX_SUCCESS, "PMIX_fence", 0);

    PMIX_PROC_LOAD(&tmp_proc, proc.nspace, 0);
    p_ret = PMIx_Get(&tmp_proc, drc_pmix_key, NULL, 0, &val_p);
    ASSERT(p_ret == PMIX_SUCCESS, "PMIx_Get", 0);
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
    char drc_key_str[256] = {0};
    int ret=0;
    int nprocs;
    int rank;
    ssg_group_id_t gid;
    ssg_group_config_t g_conf = SSG_GROUP_CONFIG_INITIALIZER;
    int c;
    char *proto=NULL;
    char *statefile=NULL;
    char *jsonfile=NULL;
    int bufsize=1024;
    int xfersize=1024;
    int nstreams=4;


    while ( (c = getopt(argc, argv, "p:b:s:t:f:x:j:" )) != -1) {
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
            case 'f':
                statefile = strdup(optarg);
                break;
            case 'j':
                jsonfile = strdup(optarg);
                break;
            case 'x':
                xfersize = atoi(optarg);
                break;
            default:
                printf("usage: %s [-p address] [-b buffer_size] [-t threads] [-s streams] [-f statefile] [-j json-config] [-x xfersize]\n", argv[0]);
                exit(-1);
        }
    }


#ifdef USE_PMIX
    pmix_proc_t proc;
    pmix_status_t p_ret;
    p_ret = PMIx_Init(&proc, NULL, 0);
    ASSERT(p_ret == PMIX_SUCCESS, "PMIx_Init failed (ret = %d)\n", p_ret);
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


    /* margo rpc thread count:
     * -1 puts all our rpcs in the mercury context
     *  0 is only for clients (processes not expecting incomming rpcs)
     *  1 will leave us one thread for the cache manager but no threads for incomming RPCS
     *  so 2 or more threads (or -1 threads) needed */
    if (nstreams == 1) {
        nstreams = 2;
        printf("Requested streams too small.  Overriding to %d\n", nstreams);
    }

    json_object *margo_json=NULL, *abtio_json=NULL;
    if (jsonfile != NULL) {
	    /* our json file has two objects: one for margo and one for abt-io.
             * cannot just hand the whole thing to either component */
            char * json_cfg = readfile(jsonfile);
            json_object *full_json = json_tokener_parse(json_cfg);
            if (full_json != NULL) {
                if (!json_object_object_get_ex(full_json, "margo", &margo_json)) {
                    fprintf(stderr, "Unable to find Margo object in json config\n");
                }
                if (!json_object_object_get_ex(full_json, "abt-io", &abtio_json)) {
                    fprintf(stderr, "Unable to find ABT-IO object in json config\n");
                }
            }
    }

#ifdef HAVE_MARGO_INIT_EXT
    struct margo_init_info minfo = {0};
    if (margo_json != NULL) {
         minfo.json_config = json_object_to_json_string(margo_json);
    } else {
        char json[512] = {0};

        minfo.json_config = json;
        /* default argobots pool kind is "fifo_wait".  "prio_wait" gives priority
         * to in-progress rpcs */
        sprintf(json, "{\"argobots\":{\"pools\":[{\"name\":\"__rpc__\", "
                "\"kind\":\"prio_wait\" }]},"
                "\"mercury\":{\"auth_key\":\"%s\"},"
                "\"rpc_thread_count\":%d"
                "}",
                drc_key_str, nstreams);
    }

    mid = margo_init_ext(proto, MARGO_SERVER_MODE, &minfo);
#else
    struct hg_init_info hii = HG_INIT_INFO_INITIALIZER;
    hii.na_init_info.auth_key = drc_key_str;
    mid = margo_init_opt(proto, MARGO_SERVER_MODE, &hii, 0, nstreams);
#endif
    ASSERT(mid != MARGO_INSTANCE_NULL, "lmargo_init_* (mid=%d)", 0);

    margo_enable_remote_shutdown(mid);

    struct abt_io_init_info abtio_ii = {NULL, 0};
    if (abtio_json != NULL)
        abtio_ii.json_config = json_object_to_json_string(abtio_json);
    /* We used to set this "number of backing threads" via command line
     * argument to whatever is best for the underlying backing store, as
     * determined by microbenchmarking (fio). Now that setting is managed by
     * the json config */
    abtio = abt_io_init_ext(&abtio_ii);
    ASSERT(abtio != ABT_IO_INSTANCE_NULL, "abtio_init failed (abtio=%d", 0);
    margo_push_finalize_callback(mid, finalize_abtio, (void*)abtio);

    ret = ssg_init();
    ASSERT(ret == 0, "ssg_init() failed (ret = %d)\n", ret);


    /* SSG group config parameters:
     * - period length
     * - timeout
     * - subgroup member count for SWIMM
     * - any necessary transport credentials */

#ifdef USE_PMIX
    ret = ssg_group_create_pmix(mid, BV_PROVIDER_GROUP_NAME, proc, &g_conf, NULL, NULL, &gid);
    ASSERT(gid != SSG_GROUP_ID_INVALID, "ssg_group_create_pmix() failed (ret = %s)","SSG_GROUP_ID_NULL");
    if (ret != SSG_SUCCESS) {
        fprintf(stderr, "ssg_group_create_pmix() failed (ret = %d)\n", ret);
    }
#endif

#ifdef USE_MPI
    ret = ssg_group_create_mpi(mid, BV_PROVIDER_GROUP_NAME, MPI_COMM_WORLD, &g_conf, NULL, NULL, &gid);
    if (ret != SSG_SUCCESS) {
        fprintf(stderr, "ssg_group_create_mpi() failed (ret = %d)\n", ret);
    }
#endif

    finalize_args_t args = {
        .g_id = gid,
        .m_id = mid
    };
    margo_push_prefinalize_callback(mid, &finalized_ssg_group_cb, (void*)&args);


    ret = ssg_get_group_size(gid, &nprocs);
    if (ret != SSG_SUCCESS) {
        fprintf(stderr, "unable to get ssg group size (%d)\n", ret);
    }
    /* only one member of the SSG group needs to write out the file.  Clients
     * will load and deserialize the file to find this ssg group and observe it */

    if (rank == 0)
        service_config_store(statefile, gid, nprocs);

    ret = bv_svc_provider_register(mid, abtio, ABT_POOL_NULL, gid, bufsize, xfersize, &bv_id);
    free(proto);
    free(statefile);

    /* dump json out for reference.  Need to split up the margo and abt-io bits
     * at initialization but glue them back together for an input json */
    if (rank == 0) {
        printf("{\n    \"margo\":\n");
        char *cfg_str = NULL;
#ifdef HAVE_MARGO_GET_CONFIG
	cfg_str = margo_get_config(mid);
	printf("%s\n", cfg_str);
        free(cfg_str);
#endif
#ifdef HAVE_ABT_IO_GET_CONFIG
        cfg_str = abt_io_get_config(abtio);
        printf(",\n    \"abt-io\":%s}\n", cfg_str);
        free(cfg_str);
#endif
    }

    margo_wait_for_finalize(mid);
#ifdef USE_PMIX
    PMIx_Finalize(NULL, 0);
#endif

#ifdef USE_MPI
    MPI_Finalize();
#endif
}
