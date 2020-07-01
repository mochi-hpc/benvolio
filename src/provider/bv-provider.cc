#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <libgen.h>
#include <string.h>

#include <margo.h>
#include <margo-bulk-pool.h>
#include <thallium.hpp>
#include <abt-io.h>
#include <ssg.h>
#include <map>
#include <mutex>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/margo_exception.hpp>

#include <list>
#include "bv-provider.h"


#include "common.h"

#include "io_stats.h"
#include "file_stats.h"

#include "lustre-utils.h"
#include <set>

#define BENVOLIO_CACHE_MAX_N_BLOCKS 3
#define BENVOLIO_CACHE_MAX_FILE 1
#define BENVOLIO_CACHE_MAX_BLOCK_SIZE 512
#define BENVOLIO_CACHE_WRITE 1
#define BENVOLIO_CACHE_READ 0

namespace tl = thallium;

/**
 * Toolkits for describing provider's cache information.
 * Maps are all from files to cache related data structures inside Cache_file_info (more details below).
*/
typedef struct {
    std::map<std::string, std::map<off_t, std::pair<uint64_t, char*>*>*> *cache_table;
    std::map<std::string, std::set<off_t>*> *cache_update_table;
    std::map<std::string, tl::mutex*> *cache_mutex_table;
    std::map<std::string, std::vector<off_t>*> *cache_offset_list_table;
    std::map<std::string, int> *register_table;
    tl::mutex *cache_mutex;
} Cache_info;
/**
 * Toolkits for describing provider's cache information for a specific file.
*/
typedef struct {
    /* I/O file descriptors */
    abt_io_instance_id abt_id;
    int fd;
    /* This map stores all cache blocks. The key is the file offset. The first element of the pair is the # of bytes of the cache block */
    std::map<off_t, std::pair<uint64_t, char*>*> *cache_table;
    /* Store which cache block has been modified by a write operation.*/
    std::set<off_t>* cache_update_list;
    /* The order of cache blocks fetched. When we flush a block, it starts from the first element of the list*/
    std::vector<off_t>* cache_offset_list;
    tl::mutex *cache_mutex;
    int io_type;
} Cache_file_info;

static void cache_register(Cache_info cache_info, std::string file, Cache_file_info *cache_file_info);
static void cache_deregister(Cache_info cache_info, std::string file);
static void cache_init(Cache_info *cache_info);
static void cache_finalize(Cache_info cache_info);
static void cache_write_back(Cache_file_info cache_file_info);
static void cache_flush(Cache_file_info cache_file_info);
//static void cache_fetch(Cache_file_info cache_file_info, off_t file_start, uint64_t file_size, int stripe_size, int stripe_count);
static size_t cache_fetch_match(char* local_buf, Cache_file_info cache_file_info, off_t file_start, uint64_t file_size, int stripe_size, int stripe_count);

struct file_info {
    int fd;
    int flags;
};

struct io_args {
    tl::engine *engine;
    tl::endpoint ep;
    abt_io_instance_id abt_id;
    int fd;
    const tl::bulk &client_bulk;
    const margo_bulk_pool_t mr_pool;
    const size_t xfersize;
    const std::vector<off_t> &file_starts;
    const std::vector<uint64_t> &file_sizes;
    int done=0;
    unsigned int file_idx=0;
    size_t fileblk_cursor=0;
    size_t client_cursor=0;
    size_t xfered=0;
    int ults_active=0;
    ABT_mutex mutex;  /* guards the 'args' state shared across all ULTs */
    ABT_eventual eventual;

    int ret;
    /* statistics collection */
    io_stats stats;
    Cache_file_info &cache_file_info;
    int stripe_count;
    int stripe_size;

    io_args(tl::engine *eng, tl::endpoint e, abt_io_instance_id id, int f, tl::bulk &b, margo_bulk_pool_t p, size_t x,
            const std::vector<off_t> & start_vec,
            const std::vector<uint64_t> & size_vec,
            Cache_file_info cache_file_i,
            int stripe_s,
            int stripe_c):
        engine(eng),
        ep(e),
        abt_id(id),
        fd(f),
        client_bulk(b),
        mr_pool(p),
        xfersize(x),
        file_starts(start_vec),
        file_sizes(size_vec),
        cache_file_info(cache_file_i),
        stripe_count(stripe_c),
        stripe_size(stripe_s) {};

};

/* file_starts: [in] list of starting offsets in file
 * file_sizes: [in] list of blocksizes in file
 * file_idx:  [in]  where to start processing file description list
 * fileblk_cursor:[in] partial progress (if any) into a file block
 * local_bufsize: [in] how much file description we need to consume
 * new_file_index: [out] start of next file description region
 * new_file_cursor: [out] partial progress (if any) of said offset-length pair
 *
 * returns: number of bytes that would be processed
 */

static size_t calc_offsets(const std::vector<off_t> & file_starts,
        const std::vector<uint64_t> & file_sizes, unsigned int file_idx,
        size_t fileblk_cursor, size_t local_bufsize,
        unsigned int *new_file_index, size_t *new_file_cursor)
{
        /* - can't do I/O while holding the lock, but instead pretend to do so.
         *   we will consume items from the file description until we have
         *   exhausted our intermediate buffer, then update shared state (bulk
         *   offset, file index, and index into any partially processed block)
         *   accordingly.
         * - The last worker might get a client transfer smaller than the
         *   intermediate buffer.  That's OK.  We are trying to compute where the
         *   0th, 1st, ... threads should start */

    size_t file_xfer=0, buf_cursor=0, nbytes, xfered=0;
    while (file_idx < file_starts.size() && file_xfer < local_bufsize) {
        nbytes = MIN(file_sizes[file_idx] - fileblk_cursor, local_bufsize-buf_cursor);
        file_xfer += nbytes;
        if (nbytes + fileblk_cursor >= file_sizes[file_idx]) {
            file_idx++;
            fileblk_cursor = 0;
        }
        else
            fileblk_cursor += nbytes;

        if (buf_cursor+nbytes < local_bufsize)
            buf_cursor += nbytes;
        else
            buf_cursor=0;
        xfered += nbytes;
    }
    *new_file_index = file_idx;
    *new_file_cursor = fileblk_cursor;

    return xfered;
}
static void write_ult(void *_args)
{
    int turn_out_the_lights = 0;  /* only set if we determine all threads are done */
    struct io_args *args = (struct io_args *)_args;
    /* The "how far along we are in bulk transfer" state persists across every
     * loop of the "obtain buffer / do i/o" cycle */
    size_t client_xfer=0, client_cursor=0;
     /* "which block of file description" and "how far into block" also persist
      * across every loop */
    unsigned int file_idx=0;
    size_t fileblk_cursor;
    double mutex_time;

    while (args->client_cursor < args->client_bulk.size() && file_idx < args->file_starts.size() )
    {
        void *local_buffer;
        size_t local_bufsize;
        size_t buf_cursor=0;
        size_t xfered=0; // total number of bytes moved in this thread
        ssize_t nbytes;  // number of bytes for a single i/o operation
        ssize_t file_xfer=0; // actual number of bytes sent to file system
        ssize_t issued = 0; // how many bytes have we sent to abt_io.  We'll
                            // collect the actual amount of data transfered
                            // after we wait for all the operations

        hg_bulk_t local_bulk;
        hg_uint32_t actual_count;

        double bulk_time, io_time, total_io_time=0.0;
        int write_count=0;
        /* Adopting same aproach as 'bake-server.c' : we will create lots of ULTs,
         * some of which might not end up doing anything */

        /* thread blocks until region from buffer pool available */
        margo_bulk_pool_get(args->mr_pool, &local_bulk);
        /* the bulk pool only provides us with handles to local bulk regions.  Get the associated memory */
        margo_bulk_access(local_bulk, 0, args->xfersize, HG_BULK_READWRITE, 1, &local_buffer, &local_bufsize, &actual_count);
        const tl::bulk local = args->engine->wrap(local_bulk, 1);
        //margo_bulk_free(local_bulk); // thallium wrap() increments refcount
        //but triggers segfault

        mutex_time = ABT_get_wtime();
        ABT_mutex_lock(args->mutex);
        // --------------------- args->mutex held ----------------//
        mutex_time = ABT_get_wtime() - mutex_time;
        args->stats.mutex_time += mutex_time;
        /* save these three for when we actually do I/O */
        auto first_file_index = args->file_idx;
        auto first_file_cursor = args->fileblk_cursor;
        client_cursor = args->client_cursor;

        /* figure out what we would have done so we can update shared 'arg'
         * state and drop the lock */
        unsigned int new_file_idx;
        size_t new_file_cursor;
        client_xfer = calc_offsets(args->file_starts, args->file_sizes, args->file_idx, args->fileblk_cursor, local_bufsize,
                &new_file_idx, &new_file_cursor);

        /* this seems wrong... we check the 'args' struct at top of loop, but
         * also update it inside the loop.  So for whatever reason the loop
         * thought we had more work to do but then it turns out we did not */
        if (client_xfer == 0)  {
            ABT_mutex_unlock(args->mutex);
            margo_bulk_pool_release(args->mr_pool, local_bulk);
            continue;
        }

        args->file_idx = new_file_idx;
        args->fileblk_cursor = new_file_cursor;
        args->client_cursor += client_xfer;


        // --------------------- args->mutex released ----------------//
        ABT_mutex_unlock(args->mutex);
        file_idx = first_file_index;
        fileblk_cursor = first_file_cursor;

        bulk_time = ABT_get_wtime();
        // the '>>' operator moves bytes from one bulk descriptor to the
        // other, moving the smaller of the two
        // operator overloading might make this a little hard to parse at
        // first.
        // - >> and << do a bulk transfer between bulk endpoints, transfering
        //   the smallest  of the two
        // - the '()' operator will select offset and length for a bulk region
        //   if one wants a subset
        // - select a subset on the client-side bulk descriptor before
        //   associating it with a connection.
        try {
            client_xfer = args->client_bulk(client_cursor, args->client_bulk.size()-client_cursor).on(args->ep) >> local;
            //printf("client_cursor = %lld,args->client_bulk.size()-client_cursor =  %lld, client_xfer = %lld\n", (long long int) client_cursor, (long long int) args->client_bulk.size()-client_cursor, (long long int)client_cursor);
        } catch (const tl::margo_exception &err) {
            std::cerr <<"Unable to bulk get at "
                << client_cursor << " size: "
                << args->client_bulk.size()-client_cursor << std::endl;
        } catch (const tl::exception &err) {
            std::cerr << "General thallium error " << std::endl;
        } catch (...) {
            std::cerr <<" Bulk get error.  Ignoring " << std::endl;
        }
        bulk_time = ABT_get_wtime() - bulk_time;

        // when are we done?
        // - what if the client has a really long file descripton but for some reason only a small amount of memory?
        // - what if the client has a really large amount of memory but a short file description?
        // -- write returns the smaller of the two
/*
        std::list<abt_io_op_t *> ops;
        std::list<ssize_t> rets;
        io_time = ABT_get_wtime();
*/
        while (file_idx < args->file_starts.size() && issued < local_bufsize) {
            // we might be able to only write a partial block
            //rets.push_back(-1);
            nbytes = MIN(args->file_sizes[file_idx]-fileblk_cursor, client_xfer-buf_cursor);


            //cache_fetch(args->cache_file_info, args->file_starts[file_idx]+fileblk_cursor, nbytes, args->stripe_size, args->stripe_count);
            cache_fetch_match((char*)local_buffer+buf_cursor, args->cache_file_info, args->file_starts[file_idx]+fileblk_cursor, nbytes, args->stripe_size, args->stripe_count);
            file_xfer += nbytes;
/*
            abt_io_op_t * write_op = abt_io_pwrite_nb(args->abt_id, args->fd, (char*)local_buffer+buf_cursor, nbytes, args->file_starts[file_idx]+fileblk_cursor, &(rets.back()) );
            ops.push_back(write_op);
*/
            issued += nbytes;
            
            write_count++;

            if (nbytes + fileblk_cursor >= args->file_sizes[file_idx]) {
                file_idx++;
                fileblk_cursor = 0;
            }
            else
                fileblk_cursor += nbytes;

            if (buf_cursor+nbytes < client_xfer)
                buf_cursor+=nbytes;
            else
                buf_cursor=0;

            xfered += nbytes;
        }
/*
        for (auto x : ops) {
            abt_io_op_wait(x);
            abt_io_op_free(x);
        }

        io_time = ABT_get_wtime() - io_time;
        total_io_time += io_time;
*/

/*
        for (auto x: rets)
            file_xfer += x;
        ops.clear();
        rets.clear();
*/
        //fprintf(stderr, "   SERVER: ABT-IO POOL: %ld items\n", abt_io_get_pool_size(args->abt_id));

        client_cursor += client_xfer;

	margo_bulk_pool_release(args->mr_pool, local_bulk);
        ABT_mutex_lock(args->mutex);
        args->stats.write_bulk_time += bulk_time;
        args->stats.write_bulk_xfers++;
        args->stats.server_write_time += total_io_time;
        args->stats.server_write_calls += write_count;
        args->stats.bytes_written += file_xfer;
        ABT_mutex_unlock(args->mutex);
    }
    double io_time = ABT_get_wtime();
    cache_write_back(args->cache_file_info);
    args->stats.server_write_time += ABT_get_wtime() - io_time;

    ABT_mutex_lock(args->mutex);
    args->ults_active--;
    if (!args->ults_active)
        turn_out_the_lights = 1;
    ABT_mutex_unlock(args->mutex);

    if (turn_out_the_lights) {
        ABT_mutex_free(&args->mutex);
        ABT_eventual_set(args->eventual, NULL, 0);
    }
    return;
}

/* a lot like write_ult, except we do the file reads into temp buffer before an
 * RMA put */
static void read_ult(void *_args)
{
    int turn_out_the_lights = 0;  /* only set if we determine all threads are done */
    struct io_args *args = (struct io_args *)_args;
    size_t client_xfer=0, client_cursor=0;
    unsigned int file_idx=0;
    size_t fileblk_cursor;

    while (args->client_cursor < args->client_bulk.size() && file_idx < args->file_starts.size() )
    {
        void *local_buffer;
        size_t local_bufsize;
        size_t buf_cursor=0;
        size_t xfered=0, nbytes, file_xfer=0, temp;
        hg_bulk_t local_bulk;
        hg_uint32_t actual_count;

        double bulk_time, io_time, total_io_time=0.0;
        int read_count = 0;
        /* Adopting same aproach as 'bake-server.c' : we will create lots of ULTs,
         * some of which might not end up doing anything */

        /* thread blocks until region from buffer pool available */
        margo_bulk_pool_get(args->mr_pool, &local_bulk);
        /* the bulk pool only provides us with handles to local bulk regions.  Get the associated memory */
        margo_bulk_access(local_bulk, 0, args->xfersize, HG_BULK_READWRITE, 1, &local_buffer, &local_bufsize, &actual_count);
        const tl::bulk local = args->engine->wrap(local_bulk, 1);
        //margo_bulk_free(local_bulk); // thallium wrap() increments refcount,
        //but triggers segfault

        double mutex_time = ABT_get_wtime();
        ABT_mutex_lock(args->mutex);
        mutex_time = ABT_get_wtime() - mutex_time;
        args->stats.mutex_time += mutex_time;
        /* save these three for when we actually do I/O */
        auto first_file_index = args->file_idx;
        auto first_file_cursor = args->fileblk_cursor;
        client_cursor = args->client_cursor;

        unsigned int new_file_idx;
        size_t new_file_cursor;

        /* figure out what we would have done so we can update shared 'arg'
         * state and drop the lock */
        client_xfer = calc_offsets(args->file_starts, args->file_sizes, args->file_idx, args->fileblk_cursor, local_bufsize,
                &new_file_idx, &new_file_cursor);

        if (client_xfer == 0)  {
            ABT_mutex_unlock(args->mutex);
            margo_bulk_pool_release(args->mr_pool, local_bulk);
            continue;
        }

        args->file_idx = new_file_idx;
        args->fileblk_cursor = new_file_cursor;
        args->client_cursor += client_xfer;

        ABT_mutex_unlock(args->mutex);
        file_idx = first_file_index;
        fileblk_cursor = first_file_cursor;

        // see write_ult for more discussion of when we stop processing
        while (file_idx < args->file_starts.size() && file_xfer < local_bufsize) {

            // we might be able to only write a partial block
            // 'local_bufsize' here instead of 'client_xfer' because we are
            // filling the local memory buffer first and then doing the rma put
            nbytes = MIN(args->file_sizes[file_idx]-fileblk_cursor, local_bufsize-buf_cursor);

            io_time = ABT_get_wtime();

            
            // A request should be a subsection of a cache block. We figure out which block it is at.
            //cache_fetch(args->cache_file_info, args->file_starts[file_idx]+fileblk_cursor, nbytes, args->stripe_size, args->stripe_count);
            temp = cache_fetch_match((char*)local_buffer+buf_cursor, args->cache_file_info, args->file_starts[file_idx]+fileblk_cursor, nbytes, args->stripe_size, args->stripe_count);
            file_xfer += temp;
            //file_xfer += abt_io_pread(args->abt_id, args->fd, (char*)local_buffer+buf_cursor, nbytes, args->file_starts[file_idx]+fileblk_cursor);
            
            io_time = ABT_get_wtime()-io_time;
            total_io_time += io_time;
            read_count++;

            if (nbytes + fileblk_cursor >= args->file_sizes[file_idx]) {
                file_idx++;
                fileblk_cursor = 0;
            }
            else
                fileblk_cursor += nbytes;

            if (buf_cursor+nbytes < local_bufsize)
                buf_cursor+=nbytes;
            else
                buf_cursor=0;

            xfered += nbytes;
        }

        // the '<<' operator moves bytes from one bulk descriptor to the
        // other, moving the smaller of the two.
        // operator overloading might make this a little hard to parse at
        // first.
        // - >> and << do a bulk transfer between bulk endpoints, transfering
        //   the smallest  of the two
        // - the '()' operator will select offset and length for a bulk region
        //   if one wants a subset
        // - select a subset on the client-side bulk descriptor before
        //   associating it with a connection.

        bulk_time = ABT_get_wtime();
        try {
            client_xfer = args->client_bulk(client_cursor, args->client_bulk.size()-client_cursor).on(args->ep) << local;
        } catch (const tl::margo_exception &err) {
            std::cerr << "Unable to bulk put at " << client_cursor
                << " size: " << args->client_bulk.size()-client_cursor
                << std::endl;
        } catch (const tl::exception &err) {
            std::cerr << "General thallium error " << std::endl;
        } catch (...) {
            std::cerr << "Bulk put problem. Ignoring. " << std::endl;
        }
        bulk_time = ABT_get_wtime() - bulk_time;

        ABT_mutex_lock(args->mutex);
        args->xfered += xfered;
        args->stats.read_bulk_time += bulk_time;
        args->stats.read_bulk_xfers++;
        args->stats.server_read_time += total_io_time;
        args->stats.server_read_calls += read_count;
        args->stats.bytes_read += file_xfer;
        ABT_mutex_unlock(args->mutex);

        client_cursor += client_xfer;

	margo_bulk_pool_release(args->mr_pool, local_bulk);
    }

    ABT_mutex_lock(args->mutex);
    args->ults_active--;
    if (!args->ults_active)
        turn_out_the_lights = 1;
    ABT_mutex_unlock(args->mutex);

    if (turn_out_the_lights) {
        ABT_mutex_free(&args->mutex);
        ABT_eventual_set(args->eventual, NULL, 0);
    }
    return;
}

static void cache_register(Cache_info cache_info, std::string file, Cache_file_info *cache_file_info) {
    std::lock_guard<tl::mutex> guard(*(cache_info.cache_mutex));
    if (cache_info.cache_table->find(file) == cache_info.cache_table->end()) {
        cache_file_info->cache_table = new std::map<off_t, std::pair<uint64_t, char*>*>;
        cache_file_info->cache_update_list = new std::set<off_t>;
        cache_file_info->cache_mutex = new tl::mutex;
        cache_file_info->cache_offset_list = new std::vector<off_t>;

        cache_info.cache_table[0][file] = cache_file_info->cache_table;
        cache_info.cache_update_table[0][file] = cache_file_info->cache_update_list;
        cache_info.cache_mutex_table[0][file] = cache_file_info->cache_mutex;
        cache_info.cache_offset_list_table[0][file] = cache_file_info->cache_offset_list;
        cache_info.register_table[0][file] = 1;
    } else {
        cache_file_info->cache_table = cache_info.cache_table[0][file];
        cache_file_info->cache_update_list = cache_info.cache_update_table[0][file];
        cache_file_info->cache_mutex = cache_info.cache_mutex_table[0][file];
        cache_file_info->cache_offset_list = cache_info.cache_offset_list_table[0][file];
        cache_info.register_table[0][file] += 1;
    }
}

static void cache_deregister(Cache_info cache_info, std::string file) {
    std::lock_guard<tl::mutex> guard(*(cache_info.cache_mutex));
    cache_info.register_table[0][file] -= 1;
    if (cache_info.cache_table->size() > BENVOLIO_CACHE_MAX_FILE && cache_info.register_table[0][file] == 0) {
        std::map<off_t, std::pair<uint64_t, char*>*>::iterator it2;
        std::map<off_t, std::pair<uint64_t, char*>*> *cache_file_table = cache_info.cache_table[0][file];
        for ( it2 = cache_file_table->begin(); it2 != cache_file_table->end(); ++it2 ) {
            free(it2->second->second);
            delete it2->second;
        }
        delete cache_file_table;
        delete cache_info.cache_update_table[0][file];
        delete cache_info.cache_mutex_table[0][file];
        delete cache_info.cache_offset_list_table[0][file];
        cache_info.cache_table->erase(file);
        cache_info.cache_mutex_table->erase(file);
        cache_info.cache_update_table->erase(file);
        cache_info.cache_offset_list_table->erase(file);
    }

}

static void cache_init(Cache_info *cache_info) {
    cache_info->cache_table = new std::map<std::string, std::map<off_t, std::pair<uint64_t, char*>*>*>;
    cache_info->cache_update_table = new std::map<std::string, std::set<off_t>*>;
    cache_info->cache_mutex_table = new std::map<std::string, tl::mutex*>;
    cache_info->cache_offset_list_table = new std::map<std::string, std::vector<off_t>*>;
    cache_info->register_table = new std::map<std::string, int>;
    cache_info->cache_mutex = new tl::mutex;
}

static void cache_finalize(Cache_info cache_info) {
    std::map<std::string, std::map<off_t, std::pair<uint64_t, char*>*>*>::iterator it;
    for ( it = cache_info.cache_table->begin(); it != cache_info.cache_table->end(); ++it ) {
        std::map<off_t, std::pair<uint64_t, char*>*>::iterator it2;
        for ( it2 = it->second->begin(); it2 != it->second->end(); ++it2 ) {
            free(it2->second->second);
            delete it2->second;
        }
        delete it->second;
    }
    std::map<std::string, std::set<off_t>*>::iterator it3;
    for (it3 = cache_info.cache_update_table->begin(); it3 != cache_info.cache_update_table->end(); ++it3) {
        delete it3->second;
    }
    std::map<std::string, tl::mutex*>::iterator it4;
    for (it4 = cache_info.cache_mutex_table->begin(); it4 != cache_info.cache_mutex_table->end(); ++it4) {
        delete it4->second;
    }
    std::map<std::string, std::vector<off_t>*>::iterator it5;
    for (it5 = cache_info.cache_offset_list_table->begin(); it5 != cache_info.cache_offset_list_table->end(); ++it5) {
        delete it5->second;
    }
    delete cache_info.cache_table;
    delete cache_info.cache_update_table;
    delete cache_info.cache_mutex_table;
    delete cache_info.cache_offset_list_table;
    delete cache_info.cache_mutex;
    delete cache_info.register_table;
}


static void cache_write_back(Cache_file_info cache_file_info) {
    std::lock_guard<tl::mutex> guard(*(cache_file_info.cache_mutex));
    std::list<abt_io_op_t *> ops;
    std::list<ssize_t> rets;
    off_t cache_offset;
    
    std::set<off_t>::iterator it;
    for (it = cache_file_info.cache_update_list->begin(); it != cache_file_info.cache_update_list->end(); ++it) {
        cache_offset = *it;
        if (cache_file_info.cache_table->find(cache_offset) == cache_file_info.cache_table->end()) {
            printf("offset not find in cache, critical error\n");
            break;
        }

        rets.push_back(-1);
        abt_io_op_t * write_op = abt_io_pwrite_nb(cache_file_info.abt_id, cache_file_info.fd, cache_file_info.cache_table[0][cache_offset]->second, cache_file_info.cache_table[0][cache_offset]->first, cache_offset, &(rets.back()) );
        ops.push_back(write_op);
    }

    for (auto x : ops) {
        abt_io_op_wait(x);
        abt_io_op_free(x);
    }
    cache_file_info.cache_update_list->clear();
    ops.clear();
    rets.clear();
}

/*
 * This function is not thread-safe, so it should be called by a thread-safe function.
 * Flush a single cache block into memory.
 * TODO: Flush many blocks with non-blocking call.
*/
static void cache_flush(Cache_file_info cache_file_info) {
    off_t cache_offset = cache_file_info.cache_offset_list[0][0];
    if (cache_file_info.cache_update_list->find(cache_offset) != cache_file_info.cache_update_list->end()) {
        ssize_t ret;
        abt_io_op_t * write_op = abt_io_pwrite_nb(cache_file_info.abt_id, cache_file_info.fd, cache_file_info.cache_table[0][cache_offset]->second, cache_file_info.cache_table[0][cache_offset]->first, cache_offset, &ret );
        abt_io_op_wait(write_op);
        abt_io_op_free(write_op);
        cache_file_info.cache_update_list->erase(cache_offset);
    }
    cache_file_info.cache_offset_list->erase(cache_file_info.cache_offset_list->begin());
    cache_file_info.cache_table->erase(cache_offset);
}

/*
 * This function takes a contiguous request block and fetch the corresponding cache into my current memory if necessary.
*/
static void cache_fetch(Cache_file_info cache_file_info, off_t file_start, uint64_t file_size, int stripe_size, int stripe_count) {
    //std::lock_guard<tl::mutex> guard(*(cache_file_info.cache_mutex));
    int my_provider;
    uint64_t block_index, subblock_index;
    size_t actual;
    int i, cache_size, cache_size2, cache_blocks;
    if (BENVOLIO_CACHE_MAX_BLOCK_SIZE > stripe_size) {
        cache_size = stripe_size;
    } else {
        cache_size = BENVOLIO_CACHE_MAX_BLOCK_SIZE;
    }

    // We can assume that non-contiguous block does not span across two Lustre stripes because the remote client should have called ad_lustre_calc_my_req kind of function to chop this kind of requests. However, it does not harm to be extra careful here.
    my_provider = ((file_start % (stripe_size * stripe_count)) / stripe_size);
    //block_index is from 0, 1, 2, 3..... We can multiply this by a whole lustre stripe to work out the beginning of caches.
    block_index = file_start / (stripe_size * stripe_count);
    subblock_index = (file_start % stripe_size) / cache_size;
    // Simplified from cache_blocks = ((file_start + file_size-1) %stripe_size + 1 + cache_size - 1) / cache_size;
    cache_blocks = ((file_start + file_size-1) %stripe_size + cache_size) / cache_size;
    for ( i = (int)subblock_index; i < cache_blocks; ++i ) {
        if ( cache_file_info.cache_table->find(block_index * stripe_size * stripe_count + i * cache_size) == cache_file_info.cache_table->end() ) {
            if (cache_file_info.cache_offset_list->size() == BENVOLIO_CACHE_MAX_N_BLOCKS) {
                cache_flush(cache_file_info);
            }
            cache_file_info.cache_offset_list->push_back(block_index * stripe_size * stripe_count + i * cache_size);
            /* This is the first time this cache block is accessed, we allocate memory and fetch the entire stripe to our memory*/
            cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size] = new std::pair<uint64_t, char*>;
            // We prevent caching subblock region that can exceed current stripe.
            cache_size2 = MIN(cache_size, stripe_size - i * cache_size);
            // This region is the maximum possible cache, we may not necessarily use all of it, but we can adjust size later without realloc.
            cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->second = (char*) malloc(sizeof(char) * cache_size2);
            // The last stripe does not necessarily have stripe size number of bytes, so we need to store the actual number of bytes cached (so we can do write-back later without appending garbage data).
            actual = abt_io_pread(cache_file_info.abt_id, cache_file_info.fd, cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->second, cache_size2, block_index * stripe_size * stripe_count);
            // There is one exception, what if the file does not exist and a write operation is creating it? We should give the cache enough region to cover the write operation despite the fact that the read operation responded insufficient bytes.
            if (cache_file_info.io_type == BENVOLIO_CACHE_WRITE) {
                // Last block has "remainder" number of bytes, otherwise we can have full cache size.
                if ( i == cache_blocks - 1 ) {
                    cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->first = (((file_start + file_size - 1) % stripe_size) % cache_size) + 1;
                } else {
                    cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->first = cache_size2;
                }
            } else if (cache_file_info.io_type == BENVOLIO_CACHE_READ) {
                cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->first = (uint64_t) actual;
                //printf("setting cache size to be %llu, cache2 = %d\n", (long long unsigned) actual, cache_size2);
            }
        } else if (cache_file_info.io_type == BENVOLIO_CACHE_WRITE) {
            // We may need to enlarge the cache array size in the last block of this stripe when a new write operation comes in because the new offset can exceed the cache domain.
            cache_size2 = MIN(cache_size, stripe_size - i * cache_size);
            if (file_start + file_size >= block_index * stripe_size * stripe_count + i * cache_size + cache_size2) {
                // Largest possible cache we can have because the file range has been extended
                cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->first = cache_size2;
            } else if (cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->first < (((file_start + file_size - 1) % stripe_size) % cache_size) + 1) {
                // Enlarge the cache accordingly if necessary
                cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->first = (((file_start + file_size - 1) % stripe_size) % cache_size) + 1;
            }
        }
    }
}

static size_t cache_match(char* local_buf, Cache_file_info cache_file_info, off_t file_start, uint64_t file_size, int stripe_size, int stripe_count) {
    //std::lock_guard<tl::mutex> guard(*(cache_file_info.cache_mutex));
    uint64_t block_index, subblock_index, actual_bytes;
    off_t cache_start;
    int i, cache_blocks, cache_size;
    uint64_t remaining_file_size = file_size;

    if (BENVOLIO_CACHE_MAX_BLOCK_SIZE > stripe_size) {
        cache_size = stripe_size;
    } else {
        cache_size = BENVOLIO_CACHE_MAX_BLOCK_SIZE;
    }
    block_index = file_start / (stripe_size * stripe_count);
    subblock_index = (file_start % stripe_size) / cache_size;
    cache_start = (file_start % stripe_size) % cache_size;

    cache_blocks = (stripe_size + cache_size - 1) / cache_size;
    for ( i = (int)subblock_index; i < cache_blocks; ++i) {
        // Cache missed, what to do? This return value -1 can catch that. However, we should never enter this condition if we call this function.
        if ( cache_file_info.cache_table->find(block_index * stripe_size * stripe_count + i * cache_size) == cache_file_info.cache_table->end() ) {
            printf("cache miss detected\n");
            return file_size - remaining_file_size;
        }
        if ( remaining_file_size > cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->first - cache_start ) {
            actual_bytes = cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->first - cache_start;
            if ( cache_file_info.io_type == BENVOLIO_CACHE_WRITE ) {
                memcpy(cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->second + cache_start, local_buf, actual_bytes);
                //Need to mark this cache block has been updated.
                cache_file_info.cache_update_list->insert(block_index * stripe_size * stripe_count + i * cache_size);
            } else if (cache_file_info.io_type == BENVOLIO_CACHE_READ){
                //printf("reading %llu number of bytes\n", (long long unsigned) actual_bytes);
                memcpy(local_buf, cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->second + cache_start, actual_bytes);
            }
            remaining_file_size -= actual_bytes;
            cache_start = 0;
            local_buf += actual_bytes;
        } else {
            if ( cache_file_info.io_type == BENVOLIO_CACHE_WRITE ) {
                memcpy(cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->second + cache_start, local_buf, remaining_file_size);
                //Need to mark this cache block has been updated
                cache_file_info.cache_update_list->insert(block_index * stripe_size * stripe_count + i * cache_size);
            } else if (cache_file_info.io_type == BENVOLIO_CACHE_READ){
                //printf("reading remaining %llu number of bytes\n", (long long unsigned) remaining_file_size);
                memcpy(local_buf, cache_file_info.cache_table[0][block_index * stripe_size * stripe_count + i * cache_size]->second + cache_start, remaining_file_size);
            }
            remaining_file_size = 0;
            break;
        }
    }
    if ( remaining_file_size ) {
        printf("Not expected to have remaining %llu bytes, the contiguous request is beyond local cache region.\n", (long long unsigned)remaining_file_size);
    }
    return file_size;
}

static size_t cache_fetch_match(char* local_buf, Cache_file_info cache_file_info, off_t file_start, uint64_t file_size, int stripe_size, int stripe_count) {
    std::lock_guard<tl::mutex> guard(*(cache_file_info.cache_mutex));
    cache_fetch(cache_file_info, file_start, file_size, stripe_size, stripe_count);
    return cache_match(local_buf, cache_file_info, file_start, file_size, stripe_size, stripe_count);
}

struct bv_svc_provider : public tl::provider<bv_svc_provider>
{
    tl::engine * engine;
    ssg_group_id_t gid;
    tl::pool pool;
    abt_io_instance_id abt_id;
    margo_bulk_pool_t mr_pool;
    std::map<std::string, file_info> filetable;      // filename to file id mapping
    const size_t bufsize;    // total size of buffer pool
    const int xfersize;      // size of one region of registered memory
    struct io_stats stats;
    static const int default_mode = 0644;
    tl::mutex    stats_mutex;
    tl::mutex    size_mutex;
    tl::mutex    fd_mutex;

    Cache_info cache_info;
    int ssg_size, ssg_rank;

    /* handles to RPC objects so we can clean them up in destructor */
    std::vector<tl::remote_procedure> rpcs;

    // server will maintain a cache of open files
    // std::map not great for LRU
    // if we see a request for a file with a different 'flags' we will close and reopen
    int getfd(const std::string &file, int flags, int mode=default_mode) {
        int fd=-1;
	std::lock_guard<tl::mutex> guard(fd_mutex);
        auto entry = filetable.find(file);
        if (entry == filetable.end() ) {
	    // no 'file' in table
            fd = abt_io_open(abt_id, file.c_str(), flags, mode);
            if (fd > 0) filetable[file] = {fd, flags};
        } else {
	    // found the file but we will close and reopen if flags are different
	    if ( entry->second.flags  == flags) {
		fd = entry->second.fd;
	    } else {
		abt_io_close(abt_id, entry->second.fd);
		fd = abt_io_open(abt_id, file.c_str(), flags, mode);
		if (fd > 0) filetable[file] = {fd, flags};
	    }
        }
        return fd;
    }

    /* write:
     * - bulk-get into a contig buffer
     * - write out to file */
    ssize_t process_write(const tl::request& req, tl::bulk &client_bulk, const std::string &file,
            const std::vector<off_t> &file_starts, const std::vector<uint64_t> &file_sizes, int stripe_size, int stripe_count)
    {
	struct io_stats local_stats;
        double write_time = ABT_get_wtime();
        unsigned i;
        /* What else can we do with an empty memory description or file
         description other than return immediately? */
        if (client_bulk.is_null() ||
                client_bulk.size() == 0 ||
                file_starts.size() == 0) {
            req.respond(0);
            write_time = ABT_get_wtime() - write_time;
            std::lock_guard<tl::mutex> guard(stats_mutex);
            stats.write_rpc_calls++;
            stats.write_rpc_time += write_time;
            return 0;
        }

        /* cannot open read-only:
         - might want to data-sieve the I/O requests
         - might later read file */
        int flags = O_CREAT|O_RDWR;
	double getfd_time = ABT_get_wtime();
        int fd = getfd(file, flags);
	getfd_time = ABT_get_wtime() - getfd_time;
	local_stats.getfd += getfd_time;
        if (fd < 0) return fd;

        /* Process cache */
        Cache_file_info cache_file_info;
        
        cache_register(cache_info, file ,&cache_file_info);
        cache_file_info.io_type = BENVOLIO_CACHE_WRITE;
        cache_file_info.fd = fd;
        cache_file_info.abt_id = abt_id;

        struct io_args args (engine, req.get_endpoint(), abt_id, fd, client_bulk, mr_pool, xfersize, file_starts, file_sizes, cache_file_info, stripe_size, stripe_count);
        ABT_mutex_create(&args.mutex);
        ABT_eventual_create(0, &args.eventual);

        // ceiling division: we'll spawn threads to operate on the registered memory.
        // (intermediate) buffer.  If we run out of
        // file description, threads will bail out early

        size_t ntimes = 1 + (client_bulk.size() -1)/xfersize;
        args.ults_active=ntimes;
        for (unsigned int i = 0; i< ntimes; i++) {
            ABT_thread_create(pool.native_handle(), write_ult, &args, ABT_THREAD_ATTR_NULL, NULL);
        }
        ABT_eventual_wait(args.eventual, NULL);

        ABT_eventual_free(&args.eventual);

        cache_deregister(cache_info, file);

        local_stats.write_response = ABT_get_wtime();
        req.respond(args.client_cursor);
        local_stats.write_response = ABT_get_wtime() - local_stats.write_response;

        local_stats += args.stats;
        local_stats.write_rpc_calls++;
        local_stats.write_rpc_time += ABT_get_wtime() - write_time;

        {
            std::lock_guard<tl::mutex> guard(stats_mutex);
            stats += local_stats;
        }
        return 0;
    }

    /* read:
     * - read into contig buffer
     * - bulk-put to client
     * as with write, might require multiple bulk-puts to complete if read
     * request larger than buffer */
    ssize_t process_read(const tl::request &req, tl::bulk &client_bulk, const std::string &file,
            std::vector<off_t> &file_starts, std::vector<uint64_t> &file_sizes, int stripe_size, int stripe_count)
    {
	struct io_stats local_stats;
        double read_time = ABT_get_wtime();
        unsigned int i;
        int my_provider, block_index;

        if (client_bulk.size() == 0 ||
                file_starts.size() == 0) {
            req.respond(0);
            std::lock_guard<tl::mutex> guard(stats_mutex);
            stats.read_rpc_calls++;
            stats.server_read_time += ABT_get_wtime() - read_time;

            return 0;
        }

        /* like with write, open for both read and write in case file opened
	 * first for read then written to. can omit O_CREAT here because
	 * reading a non-existent file would be an error */

        int flags = O_RDWR;

	double getfd_time = ABT_get_wtime();
        int fd = getfd(file, flags);
	getfd_time = ABT_get_wtime() - getfd_time;
	local_stats.getfd += getfd_time;
        if (fd < 0) return fd;

        /* Process cache */
        Cache_file_info cache_file_info;
        cache_register(cache_info, file ,&cache_file_info);
        cache_file_info.io_type = BENVOLIO_CACHE_READ;
        cache_file_info.fd = fd;
        cache_file_info.abt_id = abt_id;

        struct io_args args (engine, req.get_endpoint(), abt_id, fd, client_bulk, mr_pool, xfersize, file_starts, file_sizes, cache_file_info, stripe_size, stripe_count);

        ABT_mutex_create(&args.mutex);
        ABT_eventual_create(0, &args.eventual);
        
        // ceiling division.  Will bail out early if we exhaust file description
        size_t ntimes = 1 + (client_bulk.size() - 1)/bufsize;
        args.ults_active = ntimes;
        for (i = 0; i< ntimes; i++) {
            ABT_thread_create(pool.native_handle(), read_ult, &args, ABT_THREAD_ATTR_NULL, NULL);
        }
        ABT_eventual_wait(args.eventual, NULL);
        ABT_eventual_free(&args.eventual);

        cache_deregister(cache_info, file);

        local_stats.read_response = ABT_get_wtime();
        req.respond(args.client_cursor);
        local_stats.read_response = ABT_get_wtime() - local_stats.read_response;

        local_stats += args.stats;
        local_stats.read_rpc_calls++;
        local_stats.read_rpc_time += ABT_get_wtime() - read_time;

        {
            std::lock_guard<tl::mutex> guard(stats_mutex);
            stats += local_stats;
        }
        return 0;
    }

    struct file_stats getstats(const std::string &file)
    {
	int rc;
        struct file_stats ret;
        struct stat statbuf;
        rc = stat(file.c_str(), &statbuf);
	if (rc == -1 && errno == ENOENT) {
	    char * dup = strdup(file.c_str());
	    rc = stat(dirname(dup), &statbuf);
	    free(dup);
	}
	if (rc == 0)
	    ret.blocksize = statbuf.st_blksize;
	else
	    /* some kind of error in stat. make a reasonable guess */
	    ret.blocksize = 4096;

	/* lustre header incompatible with c++ , so need to stuff the lustre
	 * bits into a c-compiled object */
	ret.status  = lustre_getstripe(file.c_str(), &(ret.stripe_size), &(ret.stripe_count));

        return ret;
    }

    struct io_stats statistics() {

        std::lock_guard<tl::mutex> guard(stats_mutex);
        return (stats);
    }
    int del(const std::string &file) {
        int ret = abt_io_unlink(abt_id, file.c_str());
        if (ret == -1) ret = errno;
        return ret;
    }
    int flush(const std::string &file) {
	/* omiting O_CREAT: what would it mean to flush a nonexistent file ? */
        int fd = getfd(file, O_RDWR);
        return (fsync(fd));
    }

    ssize_t getsize(const std::string &file) {
	std::lock_guard<tl::mutex> guard(size_mutex);
        off_t oldpos=-1, pos=-1;
	/* have to open read-write in case subsequent write call comes in */
        int fd = getfd(file, O_CREAT|O_RDONLY);
        if (fd < 0) return fd;
        oldpos = lseek(fd, 0, SEEK_CUR);
        if (oldpos == -1)
            return -errno;
        pos = lseek(fd, 0, SEEK_END);
        if (pos == -1)
            return -errno;
        /* put things back the way we found them */
        lseek(fd, oldpos, SEEK_SET);
        return pos;
    }
    /* operations are on descriptive names but in some situations one might
     * want to separate the lookup, creation, or other overheads from the I/O
     * overheads */
    int declare(const std::string &file, int flags, int mode)
    {
        int fd = getfd(file, flags, mode);
        if (fd <  0) return fd;
        return 0;
    }


    bv_svc_provider(tl::engine *e, abt_io_instance_id abtio,
            ssg_group_id_t gid, const uint16_t provider_id, const int b, int x, tl::pool &pool)
        : tl::provider<bv_svc_provider>(*e, provider_id), engine(e), gid(gid), pool(pool), abt_id(abtio), bufsize(b), xfersize(x) {

            /* tuning: experiments will show you what the ideal transfer size
             * is for a given network.  Split up however much memory made
             * available to this provider into pool objects of that size */
            margo_bulk_pool_create(engine->get_margo_instance(), bufsize/xfersize, xfersize, HG_BULK_READWRITE, &mr_pool);

            rpcs.push_back(define("write", &bv_svc_provider::process_write, pool));
            rpcs.push_back(define("read", &bv_svc_provider::process_read, pool));
            rpcs.push_back(define("stat", &bv_svc_provider::getstats));
            rpcs.push_back(define("delete", &bv_svc_provider::del));
            rpcs.push_back(define("flush", &bv_svc_provider::flush));
            rpcs.push_back(define("statistics", &bv_svc_provider::statistics));
            rpcs.push_back(define("size", &bv_svc_provider::getsize));
            rpcs.push_back(define("declare", &bv_svc_provider::declare));
            ssg_size = ssg_get_group_size(gid);
            ssg_rank = ssg_get_group_self_rank(gid);
            cache_init(&cache_info);
        }
    void dump_io_req(const std::string extra, const tl::bulk &client_bulk, const std::vector<off_t> &file_starts, const std::vector<uint64_t> &file_sizes)
    {
        std::cout << "SERVER_REQ_DUMP:" << extra << "\n" << "   bulk size:"<< client_bulk.size() << "\n";
        std::cout << "  file offsets: " << file_starts.size() << " ";
        for (auto x : file_starts)
            std::cout<< x << " ";
        std::cout << "\n   file lengths: ";
        for (auto x: file_sizes)
            std::cout << x << " " ;
        std::cout << std::endl;
    }

    ~bv_svc_provider() {
        cache_finalize(cache_info);
        margo_bulk_pool_destroy(mr_pool);
    }
};

static void bv_on_finalize(void *args)
{
    auto provider = static_cast<bv_svc_provider_t>(args);
    for (auto x: provider->rpcs)
        x.deregister();

    delete provider->engine;
    delete provider;
}

int bv_svc_provider_register(margo_instance_id mid,
        abt_io_instance_id abtio,
        ABT_pool pool,
        ssg_group_id_t gid,
        int bufsize,
        int xfersize,
        bv_svc_provider_t *bv_id)
{
    uint16_t provider_id = 0xABC;
    auto thallium_engine = new tl::engine(mid);
    ABT_pool handler_pool;

    if (pool == ABT_POOL_NULL)
        margo_get_handler_pool(mid, &handler_pool);
    else
        handler_pool = pool;

    auto thallium_pool = tl::pool(handler_pool);

    auto bv_provider = new bv_svc_provider(thallium_engine, abtio, gid, provider_id, bufsize, xfersize, thallium_pool);
    margo_provider_push_finalize_callback(mid, bv_provider, bv_on_finalize, bv_provider);
    *bv_id = bv_provider;
    return 0;
}

