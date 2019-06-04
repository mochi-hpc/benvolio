#include <abt-io.h>
#include <margo.h>
#ifdef __cplusplus
extern "C"
{
#endif

#define MOCHIO_PROVIDER_GROUP_NAME "mochio-provider-group"

typedef struct mochio_svc_provider * mochio_svc_provider_t;

int mochio_svc_provider_register(margo_instance_id mid,
        abt_io_instance_id abtio, ABT_pool pool, ssg_group_id_t gid, mochio_svc_provider_t *mochio_id);

#ifdef __cplusplus
}
#endif
