#include <abt-io.h>
#include <margo.h>
#ifdef __cplusplus
extern "C"
{
#endif

#define BV_PROVIDER_GROUP_NAME "bv-provider-group"

typedef struct bv_svc_provider * bv_svc_provider_t;

int bv_svc_provider_register(margo_instance_id mid,
        abt_io_instance_id abtio, ABT_pool pool, ssg_group_id_t gid, int bufsize, bv_svc_provider_t *bv_id);

#ifdef __cplusplus
}
#endif
