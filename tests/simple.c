#include <string.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <mochio.h>
#include <mpi.h>

#define VERBOSE 1

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    mochio_client_t client=NULL;
    struct mochio_stats stats;
    struct iovec write_vec, read_vec;
    client = mochio_init(MPI_COMM_WORLD, argv[1]);
    char msg[] = "Hello Mochi";
    char cmp[128] = "";
    int ret = 0;
    char *filename;

#if 0
    printf("delete:\n");
    mochio_delete(client, filename);
#endif

    if (argc == 3)
        filename = argv[2];
    else
        filename = "dummy";

    printf("stat:");
    mochio_stat(client, filename, &stats);
    printf("got blocksize %ld stripe_count: %d stripe_size: %d from provider\n",
            stats.blocksize, stats.stripe_count, stats.stripe_size);

    write_vec.iov_base = msg;
    write_vec.iov_len = strlen(msg) + 1;
    off_t offset= 0;
    uint64_t size=strlen(msg);
    printf("writing\n");
    mochio_write(client, filename, 1, &write_vec, 1, &offset, &size);

    printf("flushing\n");
    mochio_flush(client, filename);

    read_vec.iov_base = cmp;
    read_vec.iov_len = 128;
    printf("reading\n");
    off_t offsets[3] = {0, 4, 8};
    uint64_t sizes[3] = {2, 2, 2};
    char compare[] = "Heo ch";
    mochio_read(client, filename, 1, &read_vec, 3, offsets, sizes);
    if (strcmp(compare, read_vec.iov_base) != 0) {
        printf("Error: Expected: %s got: %s\n", compare, (char *)read_vec.iov_base);
        ret -= -1;
    }

    printf("Longer write\n");
    int *bigbuf = malloc(15000);
    for (int i=0; i< 15000/sizeof(int); i++) bigbuf[i] = i;
    write_vec.iov_base = bigbuf;
    write_vec.iov_len = 15000;
    offset = 20;
    size = 15000;
    mochio_write(client, filename, 1, &write_vec, 1, &offset, &size);

    printf("Longer read\n");
    int *cmpbuf = malloc(15000);
    read_vec.iov_base = cmpbuf;
    read_vec.iov_len = 15000;
    offset = 20;
    size = 15000;
    mochio_read(client, filename,1, &read_vec, 1, &offset, &size);
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
    mochio_statistics(client);
#endif

    mochio_finalize(client);
    MPI_Finalize();
    return ret;
}
