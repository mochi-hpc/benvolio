#include "mochio-config.h"

#include <stdlib.h>

#include "common.h"

#ifdef HAVE_LUSTRE_LUSTREAPI_H
#include <lustre/lustreapi.h>
#endif

#ifdef HAVE_LIBLUSTREAPI
static void *alloc_lum()
{
    int v1, v3;

    v1 = sizeof(struct lov_user_md_v1) +
        LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
    v3 = sizeof(struct lov_user_md_v3) +
        LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);

    return malloc(MAX(v1, v3));
}
#endif


int lustre_getstripe(const char * filename, int32_t *stripe_size, int32_t *stripe_count)
{
    int ret = 0;
    /* guess some reasonable defaults for non-lustre systems */
    *stripe_size = 4092;
    *stripe_count = 1;

#ifdef HAVE_LIBLUSTREAPI
    struct lov_user_md *lov;
    lov = alloc_lum();

    if (llapi_file_get_stripe(filename, lov) != 0) {
	perror("Unable to get Lustre stripe info");
	return -1;
    };
    *stripe_size = lov->lmm_stripe_size;
    *stripe_count = lov->lmm_stripe_count;
#endif

    return ret;
}
