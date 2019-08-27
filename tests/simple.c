#include <string.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <bv.h>
#include <mpi.h>

#define VERBOSE 1

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    bv_client_t client=NULL;
    struct bv_stats stats;
    const char *write_address, *read_address;
    uint64_t write_size, read_size;
    client = bv_init(MPI_COMM_WORLD, argv[1]);
    char msg[] = "Hello Mochi";
    char cmp[128] = "";
    int ret = 0;
    char *filename;

#if 0
    printf("delete:\n");
    bv_delete(client, filename);
#endif

    if (argc == 3)
        filename = argv[2];
    else
        filename = "dummy";

    printf("stat:");
    bv_stat(client, filename, &stats);
    printf("got blocksize %ld stripe_count: %d stripe_size: %d from provider\n",
            stats.blocksize, stats.stripe_count, stats.stripe_size);

    write_address = msg;
    write_size = strlen(msg) + 1;
    off_t offset= 0;
    uint64_t size=strlen(msg);
    printf("writing\n");
    bv_write(client, filename, 1, &write_address, &write_size, 1, &offset, &size);

    printf("flushing\n");
    bv_flush(client, filename);

    read_address = cmp;
    read_size = 128;
    printf("reading\n");
    off_t offsets[3] = {0, 4, 8};
    uint64_t sizes[3] = {2, 2, 2};
    char compare[] = "Heo ch";
    bv_read(client, filename, 1, &read_address, &read_size, 3, offsets, sizes);
    if (strcmp(compare, read_address) != 0) {
        printf("Error: Expected: %s got: %s\n", compare, read_address);
        ret -= -1;
    }

    printf("Longer write\n");
    int *bigbuf = malloc(15000);
    for (int i=0; i< 15000/sizeof(int); i++) bigbuf[i] = i;
    write_address = (char *)bigbuf;
    write_size = 15000;
    offset = 20;
    size = 15000;
    bv_write(client, filename, 1, &write_address, &write_size, 1, &offset, &size);

    printf("Longer read\n");
    int *cmpbuf = malloc(15000);
    read_address = (char *)cmpbuf;
    read_size = 15000;
    offset = 20;
    size = 15000;
    bv_read(client, filename,1, &read_address, &read_size, 1, &offset, &size);
    for (int i=0; i< 15000/sizeof(int); i++) {
        if (bigbuf[i] != cmpbuf[i]) {
            printf("Expected %d got %d\n", bigbuf[i], cmpbuf[i]);
            ret -= -1;
            break;
        }
    }
    free(bigbuf);
    free(cmpbuf);

#if VERBOSE
    bv_statistics(client, 1);
#endif

    bv_finalize(client);
    MPI_Finalize();
    return ret;
}
