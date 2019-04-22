#include <stdint.h>
#include <unistd.h>
#include <sys/uio.h>
#include <aio.h>

typedef struct romio_client * romio_client_t;

/* easy to imagine more sophisticated distribution schemes
 * - even: hint a final file size and then divide that size across the N servers
 * - block: first N bytes to server 0, next N bytes to server 1, etc
 * - progressive: Lustre's Progressive File Layout strategy */
int romio_setchunk(char *file, ssize_t nbytes);

/* "init" might be a place to pass in distribution information too? */
int romio_init(char * protocol, char * provider, romio_client_t  client );
int romio_finalize(romio_client_t client);

/* stateless api: always pass in a file name? */
/* an iovec describes memory.  less well suited for I/O
 * - Better to have four arrays (memory offset, mem length, file offset, file length)
 * - or use a 'struct iovec' for the memory parts? */

ssize_t romio_write(char *file,
        int64_t iovcnt,
        const struct iovec iov[],
        int64_t file_count,
        const off_t file_starts[],
        uint64_t file_sizes[]);

ssize_t romio_read (char *file,
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
int romio_stat(char *filename, struct romio_stats &stats);

/*
 * If the client knows it can cork/uncork a bunch of requets, would it not make
 * more sense just to pack them all in an array for list-write and list-read ?
 * Is a cork/uncork operation more ergonimical for clients?
 */

/* stop processing for a bit while I send you a bunch of requests */
int romio_cork (char *filename);
/* go ahead and work on those requests I sent you */
int romio_uncork(char *filename);

/* might be overkill to have both a sync and a flush
 * - sync: push data to server-side memory/caches
 * - flush: ensure data persists on some kind of storage
 * - provide a list of regions so we can sync/flush a part of a file and not
 *   the whole dang thing */
int romio_sync(char *filename, int file_count,
        const off_t file_starts[],
        uint64_t file_sizes[]);
int romio_flush(char *filename);

