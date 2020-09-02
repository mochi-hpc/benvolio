#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <bv.h>


#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define VERBOSE 1

int main(int argc, char **argv)
{
    bv_client_t client=NULL;
    struct bv_stats stats;
    const char *write_address, *read_address;
    uint64_t write_size, read_size;
    bv_config_t cfg = bvutil_cfg_get(argv[1]);
    client = bv_init(cfg);
    bvutil_cfg_free(cfg);
    char msg[] = "Hello Mochi";
    char cmp[128] = "";
    uint64_t bytes;
    int ret = 0;
    char *filename;
    ssize_t filesize;

    if (argc == 3)
        filename = argv[2];
    else
        filename = "dummy";


    printf("delete:\n");
    bv_delete(client, filename);

    ret = bv_declare(client, filename, O_RDWR|O_CREAT, 0644);
    if (ret != 0) printf("Error in bv_declare\n");


    printf("stat:");
    bv_stat(client, filename, &stats);
    printf("got blocksize %ld stripe_count: %d stripe_size: %d from provider\n",
            stats.blocksize, stats.stripe_count, stats.stripe_size);

    write_address = msg;
    write_size = strlen(msg) + 1;
    off_t offset= 0;
    uint64_t size=strlen(msg);
    printf("writing\n");
    bytes = bv_write(client, filename, 1, &write_address, &write_size, 1, &offset, &size);
    if (bytes != size) {
        printf("bv_write returned unexpected number of bytes: expected %ld got %ld\n", size, bytes);
        ret -= 1;
    }

    printf("flushing\n");
    bv_flush(client, filename);

    read_address = cmp;
    read_size = 128;
    printf("reading\n");
    off_t offsets[3] = {0, 4, 8};
    uint64_t sizes[3] = {2, 2, 2};
    char compare[] = "Heo ch";
    bytes = bv_read(client, filename, 1, &read_address, &read_size, 3, offsets, sizes);
    if (strcmp(compare, read_address) != 0) {
        printf("Error: Expected: %s got: %s\n", compare, read_address);
        ret -= -1;
    }
    if (bytes != 6) {
        printf("Error: bv_read returned unexpected number of bytes: expected %d got %ld\n", 6, bytes);
        ret -=1;
    }

    printf("Longer write\n");
    int *bigbuf = malloc(15000);
    for (int i=0; i< 15000/sizeof(int); i++) bigbuf[i] = i;
    write_address = (char *)bigbuf;
    write_size = 15000;
    offset = 20;
    size = 15000;
    bytes = bv_write(client, filename, 1, &write_address, &write_size, 1, &offset, &size);
    if (bytes != size) {
        printf("Error: bv_write returned unexpected number of bytes: exptcted %ld got %ld\n", size, bytes);
        ret -=1;
    }
    printf("flushing\n");
    bv_flush(client, filename);

    printf("Longer read\n");
    int *cmpbuf = malloc(15000);
    read_address = (char *)cmpbuf;
    read_size = 15000;
    offset = 20;
    size = 15000;
    bytes = bv_read(client, filename,1, &read_address, &read_size, 1, &offset, &size);
    for (int i=0; i< 15000/sizeof(int); i++) {
        if (bigbuf[i] != cmpbuf[i]) {
            printf("Error at %d: Expected %d got %d\n", i, bigbuf[i], cmpbuf[i]);
            ret -= -1;
            break;
        }
    }
    if (bytes != size) {
        printf("Error: bv_read returned unexpected number of bytes: exptcted %ld got %ld\n", size, bytes);
        ret -=1;
    }
    free(bigbuf);
    free(cmpbuf);

    filesize = bv_getsize(client, filename);
    if (filesize != 15000+20) {
        printf("Expected %d got %ld\n", 15000+20, filesize);
        ret -= 1;
    }

#if VERBOSE
    bv_statistics(client, 1);
#endif

    bv_finalize(client);
    return ret;
}
