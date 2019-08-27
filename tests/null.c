
#include <bv.h>
#include <mpi.h>
#include <stdlib.h>

/* what if we send a null buffer to the server? */
int main(int argc, char **argv)
{
    bv_client_t client=NULL;

    int bufcount = 32768, ret;
    uint64_t size = bufcount * sizeof(int);
    char *filename;
    off_t offset=0;

    MPI_Init(&argc, &argv);

    int *writebuf;

    if (argc == 3)
        filename = argv[2];
    else
        filename = "dummy";

    client = bv_init(MPI_COMM_WORLD, argv[1]);

    writebuf = NULL;

    ret = bv_write(client, filename, 1, (const char **)&writebuf, &size, 1, &offset, &size);

    MPI_Finalize();
    return (ret != 0 ? -1 : 0);
}
