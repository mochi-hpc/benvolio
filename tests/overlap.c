#include <string.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <bv.h>
#include <mpi.h>

#define VERBOSE 1

#define NWORD 10

int main(int argc, char **argv) {
    int ret = 0;
    int i, j;
    int rank, np;
    bv_client_t client=NULL;
    struct bv_stats stats;
    const char *write_address, *read_address;
    uint64_t write_size, read_size;
    int buf[NWORD];
    int minbuf[NWORD];
    int maxbuf[NWORD];
    off_t offset;
    uint64_t len;
    char *filename;

#if 0
    printf("delete:\n");
    bv_delete(client, filename);
#endif

    MPI_Init(&argc, &argv);

    if (argc == 3)
        filename = argv[2];
    else
        filename = "dummy";

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &np);

    client = bv_init(MPI_COMM_WORLD, argv[1]);

    printf("stat:");
    bv_stat(client, filename, &stats);
    printf("got blocksize %ld stripe_count: %d stripe_size: %d from provider\n",
            stats.blocksize, stats.stripe_count, stats.stripe_size);

    printf("init write\n");
    for(i = 0; i < NWORD; i++){
        buf[i] = rank + 1;
    }
    write_address = (char*)buf;
    write_size = NWORD * sizeof(int);
    offset = 0;
    len = NWORD * sizeof(int);

    printf("writing\n");
    bv_write(client, filename, 1, &write_address, &write_size, 1, &offset, &len);

    printf("flushing\n");
    bv_flush(client, filename);

    printf("init read\n");
    
    for(i = 0; i < NWORD; i++){
        buf[i] = 0;
    }

    read_address = (char*)buf;
    read_size = NWORD * sizeof(int);
    offset = 0;
    len = NWORD * sizeof(int);

    printf("reading\n");
    bv_read(client, filename, 1, &read_address, &read_size, 1, &offset, &len);
    MPI_Allreduce(buf, maxbuf, NWORD, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(buf, minbuf, NWORD, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    for(i = 0; i < NWORD; i++){
        if (maxbuf[i] != minbuf[i]){    // All process should get the same data
            printf("Rank %d, Error: Expected: maxbuf[%d] = %d != minbuf[%d] = %d\n", rank, i, maxbuf[i], i, minbuf[i]);
            ret -= -1;
            break;
        }
        if ((i > 0) && (buf[i] != buf[i - 1])){ // Write must be atomic
            printf("Rank %d, Error: Expected: buf[%d] = %d != buf[%d] = %d\n", rank, i, buf[i], i - 1, buf[i - 1]);
            ret -= -1;
            break;
        }
    }

#if VERBOSE
    bv_statistics(client, 1);
#endif

    bv_finalize(client);
    MPI_Finalize();
    return ret;
}

