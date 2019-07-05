#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <margo.h>
#include <thallium.hpp>
#include <abt-io.h>
#include <ssg.h>
#include <map>
#include <mutex>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include "mochio-config.h"
#include "mochio-provider.h"


#include "common.h"

#include "io_stats.h"
#include "file_stats.h"
namespace tl = thallium;

#define BUFSIZE 1024

#ifdef HAVE_LUSTRE_LUSTREAPI_H
#include <lustre/lustreapi.h>

static inline int maxint(int a, int b)
{
        return a > b ? a : b;
}

static void *alloc_lum()
{
    int v1, v3;

    v1 = sizeof(struct lov_user_md_v1) +
        LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);
    v3 = sizeof(struct lov_user_md_v3) +
        LOV_MAX_STRIPE_COUNT * sizeof(struct lov_user_ost_data_v1);

    return malloc(maxint(v1, v3));
}
#endif




struct mochio_svc_provider : public tl::provider<mochio_svc_provider>
{
    tl::engine * engine;
    ssg_group_id_t gid;
    tl::pool pool;
    abt_io_instance_id abt_id;
    ssize_t blocksize=1024*8;        // todo: some kind of general distribution function perhaps
    std::map<std::string, int> filetable;      // filename to file id mapping
    // probably needs to be larger and registered with mercury somehow
    char buffer[BUFSIZE];    // intermediate buffer for read/write operations
    struct io_stats stats;
    tl::mutex    op_mutex;
    tl::mutex    stats_mutex;

    // server will maintain a cache of open files
    // std::map not great for LRU
    int getfd(const std::string &file, int flags) {
        int fd=-1;
        auto entry = filetable.find(file);
        if (entry == filetable.end() ) {
            fd = abt_io_open(abt_id, file.c_str(), flags, 0644);
            filetable[file] = fd;
        } else {
            fd = entry->second;
        }
        return fd;
    }

    /* write:
     * - bulk-get into a contig buffer
     * - write out to file */
    ssize_t process_write(const tl::request& req, tl::bulk &client_bulk, const std::string &file,
            std::vector<off_t> &file_starts, std::vector<uint64_t> &file_sizes)
    {
        double write_time = ABT_get_wtime();
        /* What else can we do with an empty memory description or file
         description other than return immediately? */
        if (client_bulk.size() == 0 ||
                file_starts.size() == 0) {
            req.respond(0);
            write_time = ABT_get_wtime() - write_time;
            std::lock_guard<tl::mutex> guard(stats_mutex);
            stats.write_rpc_calls++;
            stats.write_rpc_time += write_time;
            return 0;
        }

        double wr_mutex_time = ABT_get_wtime();
        std::lock_guard<tl::mutex> guard(op_mutex);
        wr_mutex_time = ABT_get_wtime() - wr_mutex_time;
        /* cannot open read-only:
         - might want to data-sieve the I/O requests
         - might later read file */
        int flags = O_CREAT|O_RDWR;
        int fd = getfd(file, flags);

        // we have a scratch buffer we can work with, which might be smaller
        // than whatever the client has sent our way.  We will repeatedly bulk
        // transfer into this region. We'll need to keep track of how many file
        // offset/length pairs we have processed and how far into them we are.
        // Code is going to start looking a lot like mochio...
        //
        // TODO: configurable how many segments at a time we can process
        // ?? is there a way to get all of them?
        std::vector<std::pair<void *, std::size_t>>segments(1);
        segments[0].first = (void *)(&(buffer[0]));
        segments[0].second = BUFSIZE;

        tl::endpoint ep = req.get_endpoint();
        tl::bulk local = engine->expose(segments, tl::bulk_mode::read_write);

        // when are we done?
        // - what if the client has a really long file descripton but for some reason only a small amount of memory?
        // - what if the client has a really large amount of memory but a short file description?
        // -- write returns the smaller of the two
        size_t client_xfer=0, client_cursor=0, fileblk_cursor=0, buf_cursor=0;
        size_t xfered=0, file_xfer=0, nbytes;
        unsigned int file_idx=0;
        // ceiling division: we'll do as many rounds of I/O as necessary given
        // the intermediate buffer.  Note the one exception: if we run out of
        // file description, we'll bail out early
        size_t ntimes = 1 + (client_bulk.size() -1)/BUFSIZE;

        for (unsigned int i = 0; i< ntimes; i++) {
            // the '>>' operator moves bytes from one bulk descriptor to the
            // other, moving the smaller of the two
            file_xfer = 0;
            client_xfer = client_bulk(client_cursor, client_bulk.size()-client_cursor).on(ep) >> local;
            // operator overloading might make this a little hard to parse at first.
            // - >> and << do a bulk transfer between bulk endpoints, transfering
            //   the smallest  of the two
            // - the '()' operator will select offset and length for a bulk region
            //   if one wants a subset
            // - select a subset on the client-side bulk descriptor before
            //   associating it with a connection.
            while (file_idx < file_starts.size() && file_xfer < client_xfer) {
                double pwrite_time = ABT_get_wtime();

                // we might be able to only write a partial block
                nbytes = MIN(file_sizes[file_idx]-fileblk_cursor, client_xfer-buf_cursor);
                file_xfer += abt_io_pwrite(abt_id, fd, buffer+buf_cursor, nbytes, file_starts[file_idx]+fileblk_cursor);
                {
                    std::lock_guard<tl::mutex> guard(stats_mutex);
                    stats.server_write_calls++;
                    stats.server_write_time = ABT_get_wtime() - pwrite_time;
                    stats.bytes_written += nbytes;
                }

                if (nbytes + fileblk_cursor >= file_sizes[file_idx]) {
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
            client_cursor += client_xfer;
        }
        req.respond(xfered);
        {
            std::lock_guard<tl::mutex> guard(stats_mutex);
            stats.write_rpc_calls++;
            stats.write_rpc_time += ABT_get_wtime() - write_time;
            stats.mutex_time += wr_mutex_time;
        }
        return 0;
    }

    /* read:
     * - read into contig buffer
     * - bulk-put to client
     * as with write, might require multiple bulk-puts to complete if read
     * request larger than buffer */
    ssize_t process_read(const tl::request &req, tl::bulk &client_bulk, const std::string &file,
            std::vector<off_t> &file_starts, std::vector<uint64_t> &file_sizes)
    {
        double read_time = ABT_get_wtime();

        if (client_bulk.size() == 0 ||
                file_starts.size() == 0) {
            req.respond(0);
            std::lock_guard<tl::mutex> guard(stats_mutex);
            stats.read_rpc_calls++;
            stats.server_read_time += ABT_get_wtime() - read_time;

            return 0;
        }
        double rd_mutex_time = ABT_get_wtime();
        std::lock_guard<tl::mutex> guard(op_mutex);
        rd_mutex_time = ABT_get_wtime() - rd_mutex_time;

        /* like with write, open for both read and write in case file opened
         * first for read then written to */
        int flags = O_RDWR;

        int fd = getfd(file, flags);

        tl::endpoint ep = req.get_endpoint();
        /* Simliar algorithm as write, but data movement goes in the opposite direction */
        unsigned file_idx=0;
        size_t xfered = 0, file_xfer=0, client_xfer=0, nbytes;
        size_t fileblk_cursor=0, buf_cursor=0, client_cursor=0;
        // ceiling division.  Will bail out early if we exhaust file description
        size_t ntimes = 1 + (client_bulk.size() - 1)/BUFSIZE;

        for (unsigned int i = 0; i< ntimes; i++) {
            file_xfer = 0;
            double pread_time = ABT_get_wtime();
            while (file_idx < file_starts.size() && file_xfer < BUFSIZE) {
                nbytes = MIN(file_sizes[file_idx]-fileblk_cursor, BUFSIZE-buf_cursor);
                file_xfer += abt_io_pread(abt_id, fd, buffer+buf_cursor, nbytes, file_starts[file_idx]+fileblk_cursor);
                {
                    std::lock_guard<tl::mutex> guard(stats_mutex);
                    stats.server_read_calls++;
                    stats.server_read_time = ABT_get_wtime() - pread_time;
                    stats.bytes_read += nbytes;
                }

                if (nbytes + fileblk_cursor >= file_sizes[file_idx]) {
                    file_idx++;
                    fileblk_cursor=0;
                }
                else
                    fileblk_cursor +=nbytes;

                if (buf_cursor + nbytes < BUFSIZE)
                    buf_cursor += nbytes;
                else
                    buf_cursor=0;

                xfered += nbytes;
            }
            std::vector<std::pair<void *, std::size_t>>segments(1);
            segments[0].first = (void*)(&(buffer[0]));
            segments[0].second = file_xfer;
            tl::bulk local = engine->expose(segments, tl::bulk_mode::read_write);

            client_xfer = client_bulk(client_cursor, client_bulk.size()-client_cursor).on(ep) << local;
            client_cursor += client_xfer;
        }
        req.respond(xfered);
        {
            std::lock_guard<tl::mutex> guard(stats_mutex);
            stats.read_rpc_calls++;
            stats.read_rpc_time += ABT_get_wtime() - read_time;
            stats.mutex_time += rd_mutex_time;
        }
        return 0;

    }

    struct file_stats getstats(const std::string &file)
    {
        struct file_stats ret;
        struct stat statbuf;
        stat(file.c_str(), &statbuf);
        ret.blocksize = statbuf.st_blksize;

#ifdef HAVE_LUSTRE_LUSTREAPI
        struct lov_user_md *lov;
        lov = alloc_lum();
        llapi_file_get_stripe(file, lov);
        ret.stripe_size =lov->lmm_stripe_size;
        ret.stripe_count = lov->lmm_stripe_size;
#endif

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
        int fd = getfd(file, O_RDWR);
        return (fsync(fd));
    }

    mochio_svc_provider(tl::engine *e, abt_io_instance_id abtio,
            ssg_group_id_t gid, uint16_t provider_id, tl::pool &pool)
        : tl::provider<mochio_svc_provider>(*e, provider_id), engine(e), gid(gid), pool(pool), abt_id(abtio) {

            define("write", &mochio_svc_provider::process_write, pool);
            define("read", &mochio_svc_provider::process_read, pool);
            define("stat", &mochio_svc_provider::getstats);
            define("delete", &mochio_svc_provider::del);
            define("flush", &mochio_svc_provider::flush);
            define("statistics", &mochio_svc_provider::statistics);

        }
    ~mochio_svc_provider() {
        wait_for_finalize();
    }
};

int mochio_svc_provider_register(margo_instance_id mid,
        abt_io_instance_id abtio,
        ABT_pool pool,
        ssg_group_id_t gid,
        mochio_svc_provider_t *mochio_id)
{
    uint16_t provider_id = 0xABC;
    auto thallium_engine = new tl::engine(mid);
    auto thallium_pool = tl::pool(pool);
    auto mochio_provider = new mochio_svc_provider(thallium_engine, abtio, gid, provider_id, thallium_pool);
    *mochio_id = mochio_provider;
    return 0;
}

