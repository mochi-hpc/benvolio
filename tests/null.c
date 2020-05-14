
#include <bv.h>
#include <stdlib.h>

/* what if we send a null buffer to the server? */
int main(int argc, char **argv)
{
    bv_client_t client=NULL;

    int bufcount = 32768, ret;
    uint64_t size = bufcount * sizeof(int);
    char *filename;
    off_t offset=0;


    int *writebuf;

    if (argc == 3)
        filename = argv[2];
    else
        filename = "dummy";

    bv_config_t cfg = bvutil_cfg_get(argv[1]);
    client = bv_init(cfg);
    bvutil_cfg_free(cfg);

    writebuf = NULL;

    ret = bv_write(client, filename, 1, (const char **)&writebuf, &size, 1, &offset, &size);

    return (ret != 0 ? -1 : 0);
}
