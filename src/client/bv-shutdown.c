/*
 * simple utility to trigger a graceful server shutdown
 *
 * uh, no credentials checked here so play nicely
 */

#include <bv.h>
int main(int argc, char **argv)
{
    int ret = 0;
    bv_config_t cfg = bvutil_cfg_get(argv[1]);
    bv_client_t client= bv_init(cfg);
    bvutil_cfg_free(cfg);

    ret = bv_shutdown(client);

    bv_finalize(client);
    return ret;
}
