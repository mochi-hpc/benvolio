#include <bv.h>
#include <thallium.hpp>
#include <utility>
#include <vector>
#include <ssg.h>
#include "io_stats.h"
#include "file_stats.h"
#include "calc-request.h"
#include "access.h"
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <assert.h>
namespace tl = thallium;

static int Ssg_Initialized=0;

typedef struct {
    ssg_group_id_t g_id;
    margo_instance_id m_id;
} finalize_args_t;
static void finalized_ssg_group_cb(void* data)
{
    finalize_args_t *args = (finalize_args_t *)data;
    ssg_group_unobserve(args->g_id);
    // do not finalize ssg here.  because benvolio initializes ssg before
    // margo, bv needs to finalize ssg after margo_finalize (or in this case,
    // after deleting the thallium engine
    free(args);
}


struct bv_config {
    size_t size;
    int nr_providers;
    /* we don't need to get this perfect: if we under-estimate it just means we
     * know about fewer servers until we boostrap.  50 is entirely arbitrary */
    static const int max_providers = 50;
    ssg_group_id_t group_ids[max_providers];

    /* How big is a typical ssg record?
     * - summit: 60 bytes
     * - lapotp: 50 bytes
     * - theta : 80 bytes
     * call it 200 bytes per record. */
    char buf[max_providers*200];
    bv_config() : nr_providers(max_providers) {};
};

#define MAX_PROTO_LEN 24

struct bv_client {
    tl::engine *engine = nullptr;
    std::vector<tl::provider_handle> targets;
    tl::remote_procedure read_op;
    tl::remote_procedure write_op;
    tl::remote_procedure stat_op;
    tl::remote_procedure delete_op;
    tl::remote_procedure flush_op;
    tl::remote_procedure statistics_op;
    tl::remote_procedure size_op;
    tl::remote_procedure declare_op;
    ssg_group_id_t gid;     // attaches to this group; not a member
    io_stats statistics;


    /* used to think the server would know something about how it wanted to
     * distribute data, but now that's probably best handled on a per-file
     * basis.  Pick some reasonable defaults, but don't talk to server. */
    ssize_t blocksize=1024*4;

    int stripe_size = 4096;
    int stripe_count = 1;
    int targets_used = 1;

    bv_client(tl::engine *e, ssg_group_id_t group) :
        engine(e),
        read_op(engine->define("read")),
        write_op(engine->define("write")),
        stat_op(engine->define("stat")),
        delete_op(engine->define("delete")),
        flush_op(engine->define("flush")),
        statistics_op(engine->define("statistics")),
        size_op(engine->define("size")),
        declare_op(engine->define("declare")),
        gid(group) {}

    // writing our own destructor so we can ensure nothing that needs thallium
    // to clean itself up tries to do so after thallium engine is deleted
    ~bv_client() {
        read_op.deregister();
        write_op.deregister();
        stat_op.deregister();
        delete_op.deregister();
        flush_op.deregister();
        statistics_op.deregister();
        size_op.deregister();
        declare_op.deregister();

        targets.erase(targets.begin(), targets.end());
        delete engine;
    }
};

typedef enum {
    BV_READ,
    BV_WRITE
} io_kind;

/* note: alters addr_str */
static char * get_proto_from_addr(char *addr_str)
{
    char *p = addr_str;
    char *q = strchr(addr_str, ':');
    if (q == NULL) return NULL;

    *q = '\0';
    return p;
}
bv_client_t bv_init(bv_config_t config)
{
    char *addr_str;
    int rank;
    int ret, i, nr_targets;
    char *ssg_group_buf;
    double init_time = ABT_get_wtime();


    /* we used to do a scalable read-and-broadcast inside bv_init.  I then
     * decided the scalable broadcast part of that was better handled outside
     * of benvolio.  Let the MPI-IO driver or HDF5 vol deal with exchanging
     * data among clients in whatever way it thinks is best (probably a
     * read-and-broadcast).  Complicates client a bit, but eliminates a
     * benvolio dependency on MPI -- making it easier to package and deploy.
     * Does mean we have two places where we might have to initialize ssg */

    if (!Ssg_Initialized) ssg_init();
    ssg_group_id_t ssg_gids[config->nr_providers];
    ssg_group_id_deserialize(config->buf, config->size, &config->nr_providers, ssg_gids);
    if (*ssg_gids == SSG_GROUP_ID_INVALID)
    {
        fprintf(stderr, "Error: Unable to deserialize SSG group ID\n");
        return NULL;
    }

    addr_str = ssg_group_id_get_addr_str(ssg_gids[0], 0);
    char * proto = get_proto_from_addr(addr_str);
    if (proto == NULL) return NULL;


    auto theEngine = new tl::engine(proto, THALLIUM_CLIENT_MODE);
    free(addr_str);
    bv_client *client = new bv_client(theEngine, ssg_gids[0]);

    finalize_args_t *args = (finalize_args_t *)malloc(sizeof(finalize_args_t));
    args->g_id = client->gid;

    args->m_id = client->engine->get_margo_instance();
    margo_push_prefinalize_callback(client->engine->get_margo_instance(), &finalized_ssg_group_cb, (void *)args);

    ret = ssg_group_observe(client->engine->get_margo_instance(), client->gid);
    if (ret != SSG_SUCCESS) {
        fprintf(stderr, "ssg_group attach: (%d) Is remote provider running?\n", ret);
        assert (ret == SSG_SUCCESS);
    }
    nr_targets = ssg_get_group_size(client->gid);

    for (i=0; i< nr_targets; i++) {
        tl::endpoint server(*(client->engine),
             ssg_get_group_member_addr(client->gid, ssg_get_group_member_id_from_rank(client->gid, i)),
                 0 /* do not take ownership: ssg created these handles */ );
        client->targets.push_back(tl::provider_handle(server, 0xABC));
    }

    client->statistics.client_init_time = ABT_get_wtime() - init_time;
    return client;
}

/* need to patch up the API i think: we don't know what servers to talk to.  Or
 * do you talk to one and then that provider informs the others? */
int bv_setchunk(const char *file, ssize_t nbytes)
{
    return 0;
}

int bv_delete(bv_client_t client, const char *file)
{
    return client->delete_op.on(client->targets[0])(std::string(file) );
}
int bv_finalize(bv_client_t client)
{
    if (client == NULL) return 0;
    // cleanup is kind of all over the place:
    // - we remove the client from the ssg group in a margo prefinalize callback.
    // - we finalize margo when thallium's destructor runs.
    // - We call ssg_finalize at the very end

    // ssg_finalize happens outside of the callback: in our model, we initiated
    // Argobots through ssg_init, so we need to call ssg_finalize after we tear
    // down margo
    delete client;
    ssg_finalize();

    return 0;
}

int bv_shutdown(bv_client_t client)
{
    int ret =0;
    for (auto target : client->targets)
        client->engine->shutdown_remote_engine(target);

    return ret;
}

// use bulk transfer for the memory description
// the locations in file we will just send over in a list
// - could compress the file locations: they are likely to compress quite well
// - not doing a whole lot of other data manipulation on the client
// - do need to separate the memory and file lists into per-server buckets
// -- possibly splitting up anything that falls on a server boundary
// - are the file lists monotonically non-decreasing?  Any benefit if we relax that requirement?

static size_t bv_io(bv_client_t client, const char *filename, io_kind op,
        const int64_t mem_count, const char *mem_addresses[], const uint64_t mem_sizes[],
        const int64_t file_count, const off_t file_starts[], const uint64_t file_sizes[])
{
    std::vector<io_access> my_reqs(client->targets.size());
    size_t bytes_moved = 0;

    /* How expensive is this? do we need to move this out of the I/O path?
     * Maybe 'bv_stat' can cache these values on the client struct? */
    client->targets_used = client->targets.size();
    compute_striping_info(client->stripe_size, client->stripe_count, &client->targets_used, 1);

    /* two steps:
     * - first split up the memory and file descriptions into per-target
     *   "bins".  Logic here will get fiddly: we might have to split up a memory
     *   or file block across multiple targets.
     * - second, for each target construct a bulk description for memory and
     *   send the file offset/length pairs in its bin. */
    calc_requests(mem_count, mem_addresses, mem_sizes,
            file_count, file_starts, file_sizes, client->stripe_size, client->targets_used, my_reqs);

    tl::bulk myBulk;
    auto mode = tl::bulk_mode::read_only;
    auto rpc = client->write_op;

    if (op == BV_READ) {
        mode = tl::bulk_mode::write_only;
        rpc = client->read_op;
    }

    std::vector<tl::async_response> responses;
    std::vector<tl::bulk> my_bulks;
    /* i: index into container of remote targets
     * j: index into container of bulk regions -- different because we skip
     * over targets without any work to do for this request  */
    for (unsigned int i=0, j=0; i< client->targets.size(); i++) {
        if (my_reqs[i].mem_vec.size() == 0) continue; // no work for this target

        my_bulks.push_back(client->engine->expose(my_reqs[i].mem_vec, mode));
        responses.push_back(rpc.on(client->targets[i]).async(my_bulks[j++], std::string(filename), my_reqs[i].offset, my_reqs[i].len));
    }

    for (auto &r : responses) {
        ssize_t ret = r.wait();
        if (ret >= 0)
            bytes_moved += ret;
    }
    return bytes_moved;
}

ssize_t bv_write(bv_client_t client, const char *filename,
        const int64_t mem_count, const char * mem_addresses[], const uint64_t mem_sizes[],
        const int64_t file_count, const off_t file_starts[], const uint64_t file_sizes[])
{
    ssize_t ret;
    double write_time = ABT_get_wtime();
    client->statistics.client_write_calls++;

    ret = bv_io(client, filename, BV_WRITE, mem_count, mem_addresses, mem_sizes,
            file_count, file_starts, file_sizes);

    write_time = ABT_get_wtime() - write_time;
    client->statistics.client_write_time += write_time;

    return ret;
}

ssize_t bv_read(bv_client_t client, const char *filename,
        const int64_t mem_count, const char *mem_addresses[], const uint64_t mem_sizes[],
        const int64_t file_count, const off_t file_starts[], const uint64_t file_sizes[])
{
    ssize_t ret;
    double read_time = ABT_get_wtime();
    client->statistics.client_read_calls++;

    ret = bv_io(client, filename, BV_READ, mem_count, mem_addresses, mem_sizes,
            file_count, file_starts, file_sizes);

    read_time = ABT_get_wtime() - read_time;
    client->statistics.client_read_time += read_time;

    return ret;
}

int bv_stat(bv_client_t client, const char *filename, struct bv_stats *stats)
{
    file_stats response = client->stat_op.on(client->targets[0])(std::string(filename));

    stats->blocksize = response.blocksize;
    stats->stripe_size = response.stripe_size;
    stats->stripe_count = response.stripe_count;

    /* also update client information.  This should probably be a 'map' keyed on file name */
    client->blocksize = response.blocksize;
    client->stripe_size = response.stripe_size;
    client->stripe_count = response.stripe_count;
    return(0);
}

int bv_statistics(bv_client_t client, int show_server)
{
    int ret =0;
    if (show_server) {
        for (auto target : client->targets) {
            auto s = client->statistics_op.on(target)();
            std::cout << "SERVER: ";
            io_stats(s).print_server();
        }
    }
    std::cout << "CLIENT: ";
    client->statistics.print_client();
    return ret;
}

int bv_flush(bv_client_t client, const char *filename)
{
    int ret=0;
    for (auto target : client->targets)
        ret = client->flush_op.on(target)(std::string(filename));

    return ret;
}

ssize_t bv_getsize(bv_client_t client, const char *filename)
{
    ssize_t size;
    size = client->size_op.on(client->targets[0])(std::string(filename));

    return size;
}

int bv_declare(bv_client_t client, const char *filename, int flags, int mode)
{
    std::vector<tl::async_response> responses;
    for (auto target : client->targets)
        responses.push_back(client->declare_op.on(target).async(std::string(filename), flags, mode));

    int ret = 0;
    int result = 0;
    for (auto &r : responses) {
        result = r.wait();
        ret += result;
    }
    return ret;
}

bv_config_t bvutil_cfg_get(const char *filename)
{
    bv_config_t cfg = new (bv_config);

    int ret;

    /* can't make any ssg calls until calling ssg_init(), so even though we are
     * out of the main bv_init path, we still have to set up some ssg
     * structures */
    if (!Ssg_Initialized) ssg_init();

    ret = ssg_group_id_load(filename, &cfg->nr_providers, cfg->group_ids);
    if (ret != SSG_SUCCESS) fprintf(stderr, "ssg_group_id_load: %d\n", ret);
    assert (ret == SSG_SUCCESS);
    char *buf;
    ssg_group_id_serialize(cfg->group_ids[0], cfg->nr_providers, &buf, &cfg->size);
    /* ssg_group_id_serialize internally allocated a buffer storing the group
     * information.  Get that data into our config object in a form that is
     * easily shared with other processes */
    memcpy(cfg->buf, buf, cfg->size);
    free (buf);

    return cfg;
}

ssize_t bvutil_cfg_getsize(bv_config_t cfg) {
    return (sizeof (struct bv_config));
}

void bvutil_cfg_free(bv_config_t cfg) {
    delete cfg;
}
