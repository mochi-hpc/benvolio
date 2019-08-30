/*
 * simple utility to trigger a graceful server shutdown
 *
 * uh, no credentials checked here so play nicely
 */

#include <bv.h>
int main(int argc, char **argv)
{
    int ret = 0;
    MPI_Init(&argc, &argv);
    bv_client_t client= bv_init(MPI_COMM_SELF, argv[1]);

    ret = bv_shutdown(client);

    bv_finalize(client);
    MPI_Finalize();
    return ret;
}
