#include <string.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <romio-svc.h>
#include <mpi.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    romio_client_t client=NULL;
    struct romio_stats stats;
    struct iovec write_vec, read_vec;
    client = romio_init(MPI_COMM_WORLD, argv[1]);
    char msg[] = "Hello Mochi";
    char cmp[128] = "";
    int ret = 0;

#if 0
    printf("delete:\n");
    romio_delete(client, "dummy");
#endif

    printf("stat:");
    romio_stat(client, "dummy", &stats);
    printf("got %ld from provider\n", stats.blocksize);

    write_vec.iov_base = msg;
    write_vec.iov_len = strlen(msg) + 1;
    off_t offset= 0;
    uint64_t size=strlen(msg);
    printf("writing\n");
    romio_write(client, "dummy", 1, &write_vec, 1, &offset, &size);

    printf("flushing\n");
    romio_flush(client, "dummy");

    read_vec.iov_base = cmp;
    read_vec.iov_len = 128;
    printf("reading\n");
    off_t offsets[3] = {0, 4, 8};
    uint64_t sizes[3] = {2, 2, 2};
    char compare[] = "Heo ch";
    romio_read(client, "dummy", 1, &read_vec, 3, offsets, sizes);
    if (strcmp(compare, read_vec.iov_base) != 0) {
        printf("Error: Expected: %s got: %s\n", compare, (char *)read_vec.iov_base);
        ret -= -1;
    }

    printf("Longer write\n");
    int *bigbuf = malloc(4096);
    for (int i=0; i< 1024; i++) bigbuf[i] = i;
    write_vec.iov_base = bigbuf;
    write_vec.iov_len = 4096;
    offset = 20;
    size = 4096;
    romio_write(client, "dummy", 1, &write_vec, 1, &offset, &size);

    printf("Longer read\n");
    int *cmpbuf = malloc(4096);
    read_vec.iov_base = cmpbuf;
    read_vec.iov_len = 4096;
    offset = 20;
    size = 4096;
    romio_read(client, "dummy",1, &read_vec, 1, &offset, &size);
    for (int i=0; i< 1024; i++) {
        if (bigbuf[i] != cmpbuf[i]) {
            printf("Expected %d got %d\n", bigbuf[i], cmpbuf[i]);
            ret -= -1;
            break;
        }
    }
    free(bigbuf);
    free(cmpbuf);

    romio_finalize(client);
    MPI_Finalize();
    return ret;
}
