/*
 * simple utility to let us know if benvolio providers are up and
 * running
 *
 */

#include <stdio.h>
#include <bv.h>
int main(int argc, char **argv)
{
    int ret = 0;
    bv_config_t cfg = bvutil_cfg_get(argv[1]);
    bv_client_t client= bv_init(cfg);
    bvutil_cfg_free(cfg);

    ret = bv_ping(client);
    if (ret == 0) {
        printf("benvolio UP\n");
    } else {
        printf("benvolio DOWN\n"); 
    }
    bv_finalize(client);
    return ret;
}
