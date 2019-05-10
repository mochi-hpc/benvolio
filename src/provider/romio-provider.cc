#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <margo.h>
#include <thallium.hpp>
#include <abt-io.h>
#include <ssg.h>
#include <map>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include "romio-svc-provider.h"

namespace tl = thallium;

#define BUFSIZE 1024

struct romio_svc_provider : public tl::provider<romio_svc_provider>
{
    tl::engine * engine;
    ssg_group_id_t gid;
    tl::pool pool;
    abt_io_instance_id abt_id;
    ssize_t blocksize=1024*8;        // todo: some kind of general distribution function perhaps
    std::map<std::string, int> filetable;      // filename to file id mapping
    // probably needs to be larger and registered with mercury somehow
    char buffer[BUFSIZE];    // intermediate buffer for read/write operations

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
    ssize_t process_write(const tl::request& req, tl::bulk &b, const std::string &file,
            std::vector<off_t> &file_starts, std::vector<uint64_t> &file_sizes)
    {
        /* cannot open read-only:
         - might want to data-sieve the I/O requests
         - might later read file */
        int flags = O_CREAT|O_RDWR;
        int fd = getfd(file, flags);

        // expose bulk region
        // TODO: configurable how many segments at a time we can process
        // ?? is there a way to get all of them?
        std::vector<std::pair<void *, std::size_t>>segments(1);
        segments[0].first = (void *)(&(buffer[0]));
        segments[0].second = BUFSIZE;

        tl::endpoint ep = req.get_endpoint();
        tl::bulk local = engine->expose(segments, tl::bulk_mode::read_write);

        b.on(ep) >> local;
        ssize_t xfered=0;
        for (int i = 0; i< file_starts.size(); i++)
            xfered += abt_io_pwrite(abt_id, fd, buffer+xfered, file_sizes[i], file_starts[i]);

        req.respond(xfered);
        return 0;
    }

    /* read:
     * - read into contig buffer
     * - bulk-put to client */
    ssize_t process_read(const tl::request &req, tl::bulk &b, const std::string &file,
            std::vector<off_t> &file_starts, std::vector<uint64_t> &file_sizes)
    {
        /* like with write, open for both read and write in case file opened
         * first for read then written to */
        int flags = O_RDWR;

        int fd = getfd(file, flags);
        ssize_t xfered = 0;
        for (int i= 0; i< file_starts.size(); i++) {
            int ret = abt_io_pread(abt_id, fd, buffer+xfered, file_sizes[i], file_starts[i]);
            if (ret == -1) {
                perror("pread");
                break;
            }
            xfered += ret;
        }

        std::vector<std::pair<void *, std::size_t>>segments(1);
        segments[0].first = (void*)(&(buffer[0]));
        segments[0].second = xfered;

        tl::endpoint ep = req.get_endpoint();
        tl::bulk local = engine->expose(segments, tl::bulk_mode::read_only);
        b.on(ep) << local;
        req.respond(xfered);
        return 0;
    }

    ssize_t stat(const std::string &file)
    {
        /* it should be possbile (one day) to set a block size on a per-file basis */
        return blocksize;
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

    romio_svc_provider(tl::engine *e, abt_io_instance_id abtio,
            ssg_group_id_t gid, uint16_t provider_id, tl::pool &pool)
        : tl::provider<romio_svc_provider>(*e, provider_id), engine(e), gid(gid), pool(pool), abt_id(abtio) {

            define("write", &romio_svc_provider::process_write, pool);
            define("read", &romio_svc_provider::process_read, pool);
            define("stat", &romio_svc_provider::stat);
            define("delete", &romio_svc_provider::del);
            define("flush", &romio_svc_provider::flush);

        }
    ~romio_svc_provider() {
        wait_for_finalize();
    }
};

int romio_svc_provider_register(margo_instance_id mid,
        abt_io_instance_id abtio,
        ABT_pool pool,
        ssg_group_id_t gid,
        romio_svc_provider_t *romio_id)
{
    uint16_t provider_id = 0xABC;
    auto thallium_engine = new tl::engine(mid);
    auto thallium_pool = tl::pool(pool);
    auto romio_provider = new romio_svc_provider(thallium_engine, abtio, gid, provider_id, thallium_pool);
    *romio_id = romio_provider;
    return 0;
}

