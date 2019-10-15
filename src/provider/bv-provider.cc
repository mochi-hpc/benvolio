#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <libgen.h>
#include <string.h>

#include <margo.h>
#include <thallium.hpp>
#include <abt-io.h>
#include <ssg.h>
#include <map>
#include <mutex>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/margo_exception.hpp>

#include "bv-provider.h"


#include "common.h"

#include "io_stats.h"
#include "file_stats.h"

#include "lustre-utils.h"
namespace tl = thallium;

struct file_info {
    int fd;
    int flags;
};

struct bv_svc_provider : public tl::provider<bv_svc_provider>
{
    tl::engine * engine;
    ssg_group_id_t gid;
    tl::pool pool;
    abt_io_instance_id abt_id;
    std::map<std::string, file_info> filetable;      // filename to file id mapping
    unsigned int bufsize;
    struct io_stats stats;
    static const int default_mode = 0644;
    tl::mutex    stats_mutex;
    tl::mutex    size_mutex;
    tl::mutex    fd_mutex;

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
            std::vector<off_t> &file_starts, std::vector<uint64_t> &file_sizes)
    {
	struct io_stats local_stats;
        double write_time = ABT_get_wtime();

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

	char *buffer = new char[bufsize];
        // we have a scratch buffer we can work with, which might be smaller
        // than whatever the client has sent our way.  We will repeatedly bulk
        // transfer into this region. We'll need to keep track of how many file
        // offset/length pairs we have processed and how far into them we are.
        // Code is going to start looking a lot like ROMIO...
        //
        // TODO: configurable how many segments at a time we can process
        // ?? is there a way to get all of them?
        std::vector<std::pair<void *, std::size_t>>segments(1);
        segments[0].first = (void *)(&(buffer[0]));
        segments[0].second = bufsize;

	double expose_time = ABT_get_wtime();
        tl::endpoint ep = req.get_endpoint();
        tl::bulk local = engine->expose(segments, tl::bulk_mode::read_write);
	expose_time = ABT_get_wtime() - expose_time;
	local_stats.write_expose += expose_time;

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
        size_t ntimes = 1 + (client_bulk.size() -1)/bufsize;

        for (unsigned int i = 0; i< ntimes; i++) {
            // the '>>' operator moves bytes from one bulk descriptor to the
            // other, moving the smaller of the two
            file_xfer = 0;
	    double bulk_time = ABT_get_wtime();
	    if (client_bulk.size()-client_cursor == 0) continue;
            try {
            client_xfer = client_bulk(client_cursor, client_bulk.size()-client_cursor).on(ep) >> local;
            } catch (const tl::margo_exception &err) {
                std::cerr <<"Unable to bulk get at "
                    << client_cursor << " size: "
                    << client_bulk.size()-client_cursor << std::endl;
	    } catch (const tl::exception &err) {
		std::cerr << "General thallium error " << std::endl;
            } catch (...) {
		std::cerr <<" Bulk get error.  Ignoring " << std::endl;
	    }
	    bulk_time = ABT_get_wtime() - bulk_time;
	    local_stats.write_bulk_xfers++;
	    local_stats.write_bulk_time += bulk_time;

            // operator overloading might make this a little hard to parse at first.
            // - >> and << do a bulk transfer between bulk endpoints, transfering
            //   the smallest  of the two
            // - the '()' operator will select offset and length for a bulk region
            //   if one wants a subset
            // - select a subset on the client-side bulk descriptor before
            //   associating it with a connection.

	    double pwrite_time = ABT_get_wtime();
            while (file_idx < file_starts.size() && file_xfer < client_xfer) {

                // we might be able to only write a partial block
                nbytes = MIN(file_sizes[file_idx]-fileblk_cursor, client_xfer-buf_cursor);
		file_xfer += abt_io_pwrite(abt_id, fd, buffer+buf_cursor, nbytes, file_starts[file_idx]+fileblk_cursor);
		local_stats.server_write_calls++;
		local_stats.bytes_written += nbytes;

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

	    pwrite_time = ABT_get_wtime() - pwrite_time;
	    local_stats.server_write_time += pwrite_time;
        }

	double response_time = ABT_get_wtime();
        req.respond(xfered);
	response_time = ABT_get_wtime() - response_time;
	local_stats.write_response = response_time;
	local_stats.write_rpc_calls++;
	local_stats.write_rpc_time += ABT_get_wtime() - write_time;

	local_stats.mutex_time = ABT_get_wtime();
	{
	    /* defer updating provider-wide statistics to the end so we
	     * don't have to lock the stats in the middle of i/o */
	    std::lock_guard<tl::mutex> guard(stats_mutex);
	    local_stats.mutex_time = ABT_get_wtime()-local_stats.mutex_time;
	    stats += local_stats;
	}
	delete[] buffer;

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
	struct io_stats local_stats;
        double read_time = ABT_get_wtime();

        if (client_bulk.size() == 0 ||
                file_starts.size() == 0) {
            req.respond(0);
            std::lock_guard<tl::mutex> guard(stats_mutex);
            stats.read_rpc_calls++;
            stats.server_read_time += ABT_get_wtime() - read_time;

            return 0;
        }
	char *buffer = new char[bufsize];

        /* like with write, open for both read and write in case file opened
	 * first for read then written to. can omit O_CREAT here because
	 * reading a non-existent file would be an error */

        int flags = O_RDWR;

	double getfd_time = ABT_get_wtime();
        int fd = getfd(file, flags);
	getfd_time = ABT_get_wtime() - getfd_time;
	local_stats.getfd += getfd_time;

        tl::endpoint ep = req.get_endpoint();
        /* Simliar algorithm as write, but data movement goes in the opposite direction */
        unsigned file_idx=0;
        size_t xfered = 0, file_xfer=0, client_xfer=0, nbytes;
        size_t fileblk_cursor=0, buf_cursor=0, client_cursor=0;
        // ceiling division.  Will bail out early if we exhaust file description
        size_t ntimes = 1 + (client_bulk.size() - 1)/bufsize;

        for (unsigned int i = 0; i< ntimes; i++) {
            file_xfer = 0;
            double pread_time = ABT_get_wtime();
            while (file_idx < file_starts.size() && file_xfer < bufsize) {
                nbytes = MIN(file_sizes[file_idx]-fileblk_cursor, bufsize-buf_cursor);
                file_xfer += abt_io_pread(abt_id, fd, buffer+buf_cursor, nbytes, file_starts[file_idx]+fileblk_cursor);
		local_stats.server_read_calls++;
		local_stats.bytes_read += nbytes;

                if (nbytes + fileblk_cursor >= file_sizes[file_idx]) {
                    file_idx++;
                    fileblk_cursor=0;
                }
                else
                    fileblk_cursor +=nbytes;

                if (buf_cursor + nbytes < bufsize)
                    buf_cursor += nbytes;
                else
                    buf_cursor=0;

                xfered += nbytes;
            }
	    pread_time = ABT_get_wtime() - pread_time;
	    local_stats.server_read_time += pread_time;

	    double expose_time = ABT_get_wtime();
            std::vector<std::pair<void *, std::size_t>>segments(1);
            segments[0].first = (void*)(&(buffer[0]));
            segments[0].second = file_xfer;
            tl::bulk local = engine->expose(segments, tl::bulk_mode::read_write);
	    expose_time = ABT_get_wtime() - expose_time;
	    local_stats.read_expose += expose_time;

	    double bulk_time = ABT_get_wtime();
	    if (client_bulk.size()-client_cursor != 0) {
		try {
		client_xfer = client_bulk(client_cursor, client_bulk.size()-client_cursor).on(ep) << local;
		} catch (const tl::margo_exception &err) {
		    std::cerr << "Unable to bulk put at " << client_cursor
			<< " size: " << client_bulk.size()-client_cursor
			<< std::endl;
		} catch (const tl::exception &err) {
		    std::cerr << "General thallium error " << std::endl;
		} catch (...) {
		    std::cerr << "Bulk put problem. Ignoring. " << std::endl;
		}
		client_cursor += client_xfer;
	    }
	    bulk_time = ABT_get_wtime() - bulk_time;
	    local_stats.read_bulk_xfers++;
	    local_stats.read_bulk_time += bulk_time;
        }

	double response_time = ABT_get_wtime();
        req.respond(xfered);
	response_time = ABT_get_wtime() - response_time;
	local_stats.read_response += response_time;
	local_stats.read_rpc_calls++;
	local_stats.read_rpc_time += ABT_get_wtime() - read_time;

	local_stats.mutex_time = ABT_get_wtime();
	{
	    std::lock_guard<tl::mutex> guard(stats_mutex);
	    local_stats.mutex_time = ABT_get_wtime()-local_stats.mutex_time;
	    stats+=local_stats;
        }
	delete[] buffer;
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
        if (fd == -1) return -errno;
        return 0;
    }


    bv_svc_provider(tl::engine *e, abt_io_instance_id abtio,
            ssg_group_id_t gid, uint16_t provider_id, int bufsize, tl::pool &pool)
        : tl::provider<bv_svc_provider>(*e, provider_id), engine(e), gid(gid), pool(pool), abt_id(abtio) {

            this->bufsize = bufsize;
            define("write", &bv_svc_provider::process_write, pool);
            define("read", &bv_svc_provider::process_read, pool);
            define("stat", &bv_svc_provider::getstats);
            define("delete", &bv_svc_provider::del);
            define("flush", &bv_svc_provider::flush);
            define("statistics", &bv_svc_provider::statistics);
            define("size", &bv_svc_provider::getsize);
            define("declare", &bv_svc_provider::declare);

        }
    void dump_io_req(const std::string extra, tl::bulk &client_bulk, std::vector<off_t> &file_starts, std::vector<uint64_t> &file_sizes)
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
        wait_for_finalize();
    }
};

int bv_svc_provider_register(margo_instance_id mid,
        abt_io_instance_id abtio,
        ABT_pool pool,
        ssg_group_id_t gid,
        int bufsize,
        bv_svc_provider_t *bv_id)
{
    uint16_t provider_id = 0xABC;
    auto thallium_engine = new tl::engine(mid);
    auto thallium_pool = tl::pool(pool);
    auto bv_provider = new bv_svc_provider(thallium_engine, abtio, gid, provider_id, bufsize, thallium_pool);
    *bv_id = bv_provider;
    return 0;
}

