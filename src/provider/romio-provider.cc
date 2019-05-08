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

struct romio_svc_provider : public tl::provider<romio_svc_provider>
{
    tl::engine * engine;
    ssg_group_id_t gid;
    tl::pool pool;
    abt_io_instance_id abt_id;
    ssize_t blocksize=1024*8;        // todo: some kind of general distribution function perhaps
    std::map<std::string, int> filetable;      // filename to file id mapping

    ssize_t process_io(const tl::request& req, tl::bulk &b, const std::string &file, int kind,
            std::vector<off_t> &file_starts, std::vector<uint64_t> &file_sizes)
    {
        std::cout << "Entering " << __PRETTY_FUNCTION__ << std::endl;
        // server will maintain a cache of open files
        // std::map not great for LRU
        int fd;
        auto entry = filetable.find(file);
        if (entry == filetable.end() ) {
            fd = open(file.c_str(), O_CREAT|O_RDWR, 0666);
        } else {
            fd = entry->second;
        }

        std::vector<char> buffer(1024);

        // expose bulk region
        // TODO: configurable how many segments at a time we can process
        // ?? is there a way to get all of them?
        // write: recieve regions into intermediate buffer
        std::vector<std::pair<void *, std::size_t>>segments(1);
        segments[0].first = (void *)(&(buffer[0]));
        segments[0].second = buffer.size();

        tl::endpoint ep = req.get_endpoint();
        tl::bulk local = engine->expose(segments, tl::bulk_mode::read_write);

        b.on(ep) >> local;
        buffer.resize(local.size());

        std::cout << "Server: " << buffer.size() << " bytes" << std::endl;
        ssize_t ret = write(fd, buffer.data(), buffer.size());

        // read: pull regions from file into intermediate buffer, then push to client
        return 0;
    }
    ssize_t stat(const std::string &file)
    {
        /* it should be possbile (one day) to set a block size on a per-file basis */
        return blocksize;
    }
    int del(const std::string &file) {
        int ret = unlink(file.c_str());
        if (ret == -1) ret = errno;
        return ret;
    }
    romio_svc_provider(tl::engine *e, abt_io_instance_id abtio,
            ssg_group_id_t gid, uint16_t provider_id, tl::pool &pool)
        : tl::provider<romio_svc_provider>(*e, provider_id), engine(e), gid(gid), pool(pool), abt_id(abtio) {

            define("io", &romio_svc_provider::process_io, pool);
            define("stat", &romio_svc_provider::stat);
            define("delete", &romio_svc_provider::del);

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

