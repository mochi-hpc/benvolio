#ifndef BV_SVC_H
#define BV_SVC_H

#include <stdint.h>
#include <unistd.h>
#include <aio.h>

#ifdef __cplusplus
extern "C"
{
#endif



typedef struct bv_client * bv_client_t;

typedef struct bv_config *bv_config_t;

/**
 * we leave it up to the client how to efficiently generate a bv_config object.
 * I would suggest reading the config file on one process and broadcasting the
 * result to everyone else, though for a small number of clients the "everyone
 * reads" approach will be ok.
 *
 * benvolio is providing these helper routines for obtaining group data from
 * the file
 *
 * Caller responsible for freeing `bv_config` object  with `bvutil_cfg_free()`
 */
bv_config_t bvutil_cfg_get(const char *filename);

/**
 * how big is the memory region associated with the opaque pointer?
 */
ssize_t bvutil_cfg_getsize(bv_config_t cfg);

/**
 * free the memory region associated with the opaque pointer
 */
void bvutil_cfg_free(bv_config_t cfg);

/* easy to imagine more sophisticated distribution schemes
 * - even: hint a final file size and then divide that size across the N servers
 * - block: first N bytes to server 0, next N bytes to server 1, etc
 * - progressive: Lustre's Progressive File Layout strategy */
int bv_setchunk(const char *file, ssize_t nbytes);

/* "init" might be a place to pass in distribution information too? */
bv_client_t bv_init(bv_config_t cfg);

/* clean up clients.  Must be the absolute last routine called */
int bv_finalize(bv_client_t client);


/* 'shutdown' (go ask servers to kindly exit) is sufficiently different from
 * 'finalize' (clean up client stuff) that it seemed to warrant its own api
 * call. */
int bv_shutdown(bv_client_t client);

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

/* operations are on descriptive names but in some situations one might want
 * to separate the lookup, creation, or other overheads from the I/O overheads
 */
#define BV_NOCREATE 0
int bv_declare(bv_client_t client, const char *filename, int flags, int mode);

/* ping: returns non-zero if unable to communicate with any provider */
int bv_ping(bv_client_t client);

#ifdef __cplusplus
}
#endif

#endif
