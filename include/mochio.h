#ifndef mochio_SVC_H
#define mochio_SVC_H

#include <stdint.h>
#include <unistd.h>
#include <sys/uio.h>
#include <aio.h>
#include <mpi.h>

#ifdef __cplusplus
extern "C"
{
#endif



typedef struct mochio_client * mochio_client_t;

/* easy to imagine more sophisticated distribution schemes
 * - even: hint a final file size and then divide that size across the N servers
 * - block: first N bytes to server 0, next N bytes to server 1, etc
 * - progressive: Lustre's Progressive File Layout strategy */
int mochio_setchunk(const char *file, ssize_t nbytes);

/* "init" might be a place to pass in distribution information too? */
mochio_client_t mochio_init(MPI_Comm comm, const char * ssg_statefile);
int mochio_finalize(mochio_client_t client);

/* stateless api: always pass in a file name? */
/* an iovec describes memory.  less well suited for I/O
 * - Better to have four arrays (memory offset, mem length, file offset, file length)
 * - or use a 'struct iovec' for the memory parts? */

ssize_t mochio_write(mochio_client_t client,
        const char *file,
        int64_t iovcnt,
        const struct iovec iov[],
        int64_t file_count,
        const off_t file_starts[],
        uint64_t file_sizes[]);

ssize_t mochio_read (mochio_client_t client,
        const char *file,
        int64_t iovcnt,
        const struct iovec iov[],
        int64_t file_count,
        const off_t file_starts[],
        uint64_t file_sizes[]);


/*
 * these routines seem useful, but still thinking about details
 */

/* Things we might want to collect
 * - requested file distribution strategy
 * - actual distribution of files to servers
 * - queue depth
 * ...
 */
struct mochio_stats {
    ssize_t blocksize;
    int32_t stripe_size;
    int32_t stripe_count;
};
int mochio_stat(mochio_client_t client, const char *filename, struct mochio_stats *stats);

int mochio_statistics(mochio_client_t client);

/* flush: request all cached data written to disk */
int mochio_flush(mochio_client_t client, const char *filename);

/* delete: remove the file */
int mochio_delete(mochio_client_t client, const char *filename);
#ifdef __cplusplus
}
#endif

#endif
