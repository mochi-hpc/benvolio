#include <abt-io.h>
#include <margo.h>
#ifdef __cplusplus
extern "C"
{
#endif

#define BV_PROVIDER_GROUP_NAME "bv-provider-group"

typedef struct bv_svc_provider * bv_svc_provider_t;

/**
 * mid: associate with this margo engine
 * pool: an Argobots pool to manage ULTs spawned by the provider when servicing an RPC
 * gid: the SSG group these provider instances are part of
 * bufsize: how much memory this provider can use to buffer I/O requests
 * xfersize: ideal transfer size.  Determine via experiment.
 *          Also determines how many simultaneous transfer threads will spawn
 * bv_id: identifier for this benvolio instance
 */
int bv_svc_provider_register(margo_instance_id mid,
        abt_io_instance_id abtio, ABT_pool pool, ssg_group_id_t gid, int bufsize, int xfersize, bv_svc_provider_t *bv_id);

#ifdef __cplusplus
}
#endif
