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
    size_t count = 0;
    bv_config_t cfg = bvutil_cfg_get(argv[1]);
    if (cfg == NULL) {
        fprintf(stderr, "Unable to obtain benvolio config\n");
        return -1;
    }

    bv_client_t client= bv_init(cfg);
    bvutil_cfg_free(cfg);

    ret = bv_ping(client, &count);
    if (ret == 0) {
        printf("benvolio UP: %ld providers\n", count);
    } else {
        printf("benvolio DOWN: %d of %ld providers unresponsive\n", ret, count);
    }
    bv_finalize(client);
    return ret;
}
