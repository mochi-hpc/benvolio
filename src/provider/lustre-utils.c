#include "bv-config.h"

#include <stdlib.h>
#include <errno.h>
#include <libgen.h>


#include "common.h"

#ifdef HAVE_LUSTRE_LUSTREAPI_H
#include <lustre/lustreapi.h>
#endif

#ifdef HAVE_LIBLUSTREAPI
/* there are two(!!) kinds of data the lustre library might return, so we need
 * something big enough to hold either one */
static size_t get_lumsize()
{
    int v1, v3;

    v1 = sizeof(struct lov_user_md_v1) +
        LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
    v3 = sizeof(struct lov_user_md_v3) +
        LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);

    return MAX(v1, v3);
}
#endif


int lustre_getstripe(const char * filename, int32_t *stripe_size, int32_t *stripe_count)
{
    int ret = 0;
    int status = 0;
    /* guess some reasonable defaults for non-lustre systems */
    *stripe_size = 16384444;
    *stripe_count = 156;

#ifdef HAVE_LIBLUSTREAPI
    struct lov_user_md *lov;
    lov = alloca(get_lumsize());

    /* - maybe the file wasn't there?
     *	  - check parent directory
     * - maybe the file is not lustre? */
    ret = llapi_file_get_stripe(filename, lov);
    if (ret != 0) status = errno;
    switch(status) {
	char *dup, *parent;
	case ENOTTY: /* a valid file, but not a lustre file  */
	    ret = 0;
	    goto fn_exit;
	case ENOENT:  /* given file didn't exist, but can use parent's stripe info */
	    dup = strdup(filename);
	    parent = dirname(dup);
	    ret = llapi_file_get_stripe(parent, lov);
	    /* if still enoent, we give up */
	    if (ret != 0 && errno ==  ENOENT) {
		free(dup);
		goto fn_exit;
	    }
	    /* fall through */
	case 0: /* success */
	    *stripe_size = lov->lmm_stripe_size;
	    *stripe_count = lov->lmm_stripe_count;
	    break;

	default:
	    perror("Unable to get Lustre stripe info");
    };

#endif

fn_exit:
    return ret;
}
