#include <abt-io.h>
#include <margo.h>
#ifdef __cplusplus
extern "C"
{
#endif

#define ROMIO_PROVIDER_GROUP_NAME "romio-provider-group"

typedef struct romio_svc_provider * romio_svc_provider_t;

int romio_svc_provider_register(margo_instance_id mid,
        abt_io_instance_id abtio, ABT_pool pool, ssg_group_id_t gid, romio_svc_provider_t *romio_id);

#ifdef __cplusplus
}
#endif
