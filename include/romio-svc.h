#ifndef ROMIO_SVC_H
#define ROMIO_SVC_H

#include <stdint.h>
#include <unistd.h>
#include <sys/uio.h>
#include <aio.h>
#include <mpi.h>

#ifdef __cplusplus
extern "C"
{
#endif



typedef struct romio_client * romio_client_t;

/* easy to imagine more sophisticated distribution schemes
 * - even: hint a final file size and then divide that size across the N servers
 * - block: first N bytes to server 0, next N bytes to server 1, etc
 * - progressive: Lustre's Progressive File Layout strategy */
int romio_setchunk(const char *file, ssize_t nbytes);

/* "init" might be a place to pass in distribution information too? */
romio_client_t romio_init(MPI_Comm comm, const char * protocol, const char * ssg_statefile);
int romio_finalize(romio_client_t client);

/* stateless api: always pass in a file name? */
/* an iovec describes memory.  less well suited for I/O
 * - Better to have four arrays (memory offset, mem length, file offset, file length)
 * - or use a 'struct iovec' for the memory parts? */

ssize_t romio_write(romio_client_t client,
        const char *file,
        int64_t iovcnt,
        const struct iovec iov[],
        int64_t file_count,
        const off_t file_starts[],
        uint64_t file_sizes[]);

ssize_t romio_read (romio_client_t client,
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
struct romio_stats {
    ssize_t blocksize;
};
int romio_stat(romio_client_t client, const char *filename, struct romio_stats *stats);

/* flush: request all cached data written to disk */
int romio_flush(romio_client_t client, const char *filename);

/* delete: remove the file */
int romio_delete(romio_client_t client, const char *filename);
#ifdef __cplusplus
}
#endif

#endif
