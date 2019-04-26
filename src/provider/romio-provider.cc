
#include <margo.h>
#include <thallium.hpp>
#include <abt-io.h>
#include <ssg.h>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include "romio-svc-provider.h"

namespace tl = thallium;

struct romio_svc_provider : public tl::provider<romio_svc_provider>
{
    ssg_group_id_t gid;
    tl::pool pool;
    abt_io_instance_id abt_id;

    size_t process_io(const std::string &fiie, int kind,
            std::vector<off_t> &file_starts, std::vector<uint64_t> &file_sizes)
    {
        // open file
        // expose bulk region
        // read: pull regions from file into intermediate buffer
        // write: pack regions into intermediate buffer
        return 0;
    }
    romio_svc_provider(tl::engine *e, abt_io_instance_id abtio,
            ssg_group_id_t gid, uint16_t provider_id, tl::pool &pool)
        : tl::provider<romio_svc_provider>(*e, provider_id), gid(gid), pool(pool), abt_id(abtio) {

            define("io", &romio_svc_provider::process_io, pool);
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

