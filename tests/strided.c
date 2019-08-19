#include <string.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <mochio.h>
#include <mpi.h>

#define VERBOSE 1

#define NSTRIDE 10
#define SSIZE 1024

int main(int argc, char **argv) {
    int ret = 0;
    int i, j;
    int rank, np;
    mochio_client_t client=NULL;
    struct mochio_stats stats;
    const char *write_address, *read_address;
    uint64_t write_size, read_size;
    unsigned char *buf;
    off_t offsets[NSTRIDE];
    uint64_t lens[NSTRIDE];
    char *filename;

#if 0
    printf("delete:\n");
    mochio_delete(client, filename);
#endif

    MPI_Init(&argc, &argv);

    if (argc == 3)
        filename = argv[2];
    else
        filename = "dummy";

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &np);

    client = mochio_init(MPI_COMM_WORLD, argv[1]);

    printf("stat:");
    mochio_stat(client, filename, &stats);
    printf("got blocksize %ld stripe_count: %d stripe_size: %d from provider\n",
            stats.blocksize, stats.stripe_count, stats.stripe_size);

    printf("init write\n");
    buf = (unsigned char*)malloc(NSTRIDE * SSIZE);
    for(i = 0; i < NSTRIDE * SSIZE; i++){
        buf[i] = rank + 1;
    }
    for(i = 0; i < NSTRIDE; i++){
        offsets[i] = SSIZE * (i * np + rank);
        lens[i] = SSIZE;
    }

    write_address = buf;
    write_size = NSTRIDE * SSIZE;

    printf("writing\n");
    mochio_write(client, filename, 1, &write_address, &write_size, NSTRIDE, offsets, lens);

    printf("flushing\n");
    mochio_flush(client, filename);

    printf("init read\n");
    
    for(i = 0; i < NSTRIDE * SSIZE; i++){
        buf[i] = 0;
    }

    offsets[0] = rank * NSTRIDE * SSIZE;
    lens[0] = NSTRIDE * SSIZE;

    read_address = buf;
    read_size = NSTRIDE * SSIZE;

    printf("reading\n");
    mochio_read(client, filename, 1, &read_address, &read_size, 1, offsets, lens);
    for(i = 0; i < NSTRIDE * SSIZE; i++){
        j = (i / SSIZE) % np + 1;
        if (buf[i] != j){
            printf("Rank %d, Error: Expected: buf[%d] = %d got: %d\n", rank, i, j, buf[i]);
            ret -= -1;
            break;
        }
    }

    free(buf);

#if VERBOSE
    mochio_statistics(client, 1);
#endif

    mochio_finalize(client);
    MPI_Finalize();
    return ret;
}

