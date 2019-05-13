#include <string.h>
#include <mpi.h>
#include <stdio.h>
#include <romio-svc.h>
#include <mpi.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    romio_client_t client=NULL;
    struct romio_stats stats;
    struct iovec write_vec, read_vec;
    client = romio_init(MPI_COMM_WORLD, argv[1], argv[2]);
    char msg[] = "Hello Mochi";
    char cmp[128];

    printf("delete:\n");
    romio_delete(client, "dummy");

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
    romio_read(client, "dummy", 1, &read_vec, 3, offsets, sizes);
    printf("read back %s\n", read_vec.iov_base);

    romio_finalize(client);
    MPI_Finalize();
}
