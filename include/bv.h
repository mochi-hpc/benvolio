#ifndef BV_SVC_H
#define BV_SVC_H

#include <stdint.h>
#include <unistd.h>
#include <aio.h>
#include <mpi.h>

#ifdef __cplusplus
extern "C"
{
#endif



typedef struct bv_client * bv_client_t;

/* easy to imagine more sophisticated distribution schemes
 * - even: hint a final file size and then divide that size across the N servers
 * - block: first N bytes to server 0, next N bytes to server 1, etc
 * - progressive: Lustre's Progressive File Layout strategy */
int bv_setchunk(const char *file, ssize_t nbytes);

/* "init" might be a place to pass in distribution information too? */
bv_client_t bv_init(MPI_Comm comm, const char * ssg_statefile);
int bv_finalize(bv_client_t client);

/* stateless api: always pass in a file name? */

ssize_t bv_write(bv_client_t client,
        const char *file,
        const int64_t mem_count,
        const char *mem_addresses[],
        const uint64_t mem_sizes[],
        const int64_t file_count,
        const off_t file_starts[],
        const uint64_t file_sizes[]);

ssize_t bv_read (bv_client_t client,
        const char *file,
        const int64_t mem_count,
        const char *mem_addresses[],
        const uint64_t mem_sizes[],
        const int64_t file_count,
        const off_t file_starts[],
        const uint64_t file_sizes[]);


/*
 * these routines seem useful, but still thinking about details
 */

/* Things we might want to collect
 * - requested file distribution strategy
 * - actual distribution of files to servers
 * - queue depth
 * ...
 */
struct bv_stats {
    ssize_t blocksize;
    int32_t stripe_size;
    int32_t stripe_count;
};
int bv_stat(bv_client_t client, const char *filename, struct bv_stats *stats);

/**
 * if `show_server` set, statistics will also include information from every remote target
 * Use case: at end of MPI job, every process will want to show client statistis
 * but only one process will want to show the server information
 */
int bv_statistics(bv_client_t client, int show_server);

/* flush: request all cached data written to disk */
int bv_flush(bv_client_t client, const char *filename);

/* delete: remove the file */
int bv_delete(bv_client_t client, const char *filename);

/* getting file size: on parallel file system file size is expensive so make this a separate routine */
ssize_t bv_getsize(bv_client_t client, const char *filename);

#ifdef __cplusplus
}
#endif

#endif
