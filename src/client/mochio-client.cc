#include <mochio.h>
#include <thallium.hpp>
#include <utility>
#include <vector>
#include <ssg.h>
#include <mpi.h>
#include "io_stats.h"
#include "file_stats.h"
#include "calc-request.h"
#include "access.h"
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <assert.h>
namespace tl = thallium;

#define MAX_PROTO_LEN 24

struct mochio_client {
    tl::engine *engine;
    std::vector<tl::provider_handle> targets;
    char proto[MAX_PROTO_LEN];
    tl::remote_procedure read_op;
    tl::remote_procedure write_op;
    tl::remote_procedure stat_op;
    tl::remote_procedure delete_op;
    tl::remote_procedure flush_op;
    tl::remote_procedure statistics_op;
    ssg_group_id_t gid;     // attaches to this group; not a member
    io_stats statistics;

    ssize_t blocksize=1024*4; // TODO: make more dynamic

    int stripe_size;
    int stripe_count;
    int targets_used;
};

typedef enum {
    MOCHIO_READ,
    MOCHIO_WRITE
} io_kind;

static int set_proto_from_addr(mochio_client_t client, char *addr_str)
{
    int i;
    for (i=0; i< MAX_PROTO_LEN; i++) {
        if (addr_str[i] == ':') {
            client->proto[i] = '\0';
            break;
        }
        client->proto[i] = addr_str[i];
    }
    if (client->proto[i] != '\0') return -1;
    return 0;
}
mochio_client_t mochio_init(MPI_Comm comm, const char * cfg_file)
{
    char *addr_str;
    int rank;
    int ret, i, nr_targets;
    struct mochio_client * client = (struct mochio_client *)calloc(1,sizeof(*client));
    char *ssg_group_buf;
    double init_time = ABT_get_wtime();


    /* scalable read-and-broadcast of group information: only one process reads
     * cfg from file system.  These routines can all be called before ssg_init */
    MPI_Comm_rank(comm, &rank);
    uint64_t ssg_serialize_size;
    ssg_group_buf = (char *) malloc(1024); // how big do these get?
    if (rank == 0) {
        ret = ssg_group_id_load(cfg_file, &(client->gid));
        assert (ret == SSG_SUCCESS);
        ssg_group_id_serialize(client->gid, &ssg_group_buf, &ssg_serialize_size);
    }
    MPI_Bcast(&ssg_serialize_size, 1, MPI_UINT64_T, 0, comm);
    MPI_Bcast(ssg_group_buf, ssg_serialize_size, MPI_CHAR, 0, comm);
    ssg_group_id_deserialize(ssg_group_buf, ssg_serialize_size, &(client->gid));
    addr_str = ssg_group_id_get_addr_str(client->gid);
    if (set_proto_from_addr(client, addr_str) != 0) return NULL;

    /* This is very c-like.  probably needs some C++ RAII thing here */
    client->engine = new tl::engine(client->proto, THALLIUM_CLIENT_MODE);
    ssg_init(client->engine->get_margo_instance());

    ret = ssg_group_attach(client->gid);
    if (ret != SSG_SUCCESS) {
        fprintf(stderr, "ssg_group attach: (%d)\n", ret);
        assert (ret == SSG_SUCCESS);
    }
    nr_targets = ssg_get_group_size(client->gid);

    for (i=0; i< nr_targets; i++) {
        tl::endpoint server(*(client->engine), ssg_get_addr(client->gid, i) );
        client->targets.push_back(tl::provider_handle(server, 0xABC));
    }

    client->read_op = client->engine->define("read");
    client->write_op = client->engine->define("write");
    client->stat_op = client->engine->define("stat");
    client->delete_op = client->engine->define("delete");
    client->flush_op = client->engine->define("flush");
    client->statistics_op = client->engine->define("statistics");


    /* used to think the server would know something about how it wanted to
     * distribute data, but now that's probably best handled on a per-file
     * basis.  Pick some reasonable defaults, but don't talk to server. */
    client->blocksize = 4096;
    client->stripe_size= 4096;
    client->stripe_count=1;

    free(addr_str);

    client->statistics.client_init_time = ABT_get_wtime() - init_time;
    return client;
}

/* need to patch up the API i think: we don't know what servers to talk to.  Or
 * do you talk to one and then that provider informs the others? */
int mochio_setchunk(const char *file, ssize_t nbytes)
{
    return 0;
}

int mochio_delete(mochio_client_t client, const char *file)
{
    return client->delete_op.on(client->targets[0])(std::string(file) );
}
int mochio_finalize(mochio_client_t client)
{
    ssg_group_detach(client->gid);
    ssg_finalize();
    delete client->engine;
    free(client);
    return 0;
}

// use bulk transfer for the memory description
// the locations in file we will just send over in a list
// - could compress the file locations: they are likely to compress quite well
// - not doing a whole lot of other data manipulation on the client
// - do need to separate the memory and file lists into per-server buckets
// -- possibly splitting up anything that falls on a server boundary
// - are the file lists monotonically non-decreasing?  Any benefit if we relax that requirement?

static size_t mochio_io(mochio_client_t client, const char *filename, io_kind op,
        const int64_t mem_count, const char *mem_addresses[], const uint64_t mem_sizes[],
        const int64_t file_count, const off_t file_starts[], const uint64_t file_sizes[])
{
    std::vector<struct access> my_reqs(client->targets.size());
    size_t bytes_moved = 0;

    /* How expensive is this? do we need to move this out of the I/O path?
     * Maybe 'mochio_stat' can cache these values on the client struct? */
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

    if (op == MOCHIO_READ) {
        mode = tl::bulk_mode::write_only;
        rpc = client->read_op;
    }

    std::vector<tl::async_response> responses;
    for (unsigned int i=0; i< client->targets.size(); i++) {
        if (my_reqs[i].mem_vec.size() == 0) continue; // no work for this target

        myBulk = client->engine->expose(my_reqs[i].mem_vec, mode);
        responses.push_back(rpc.on(client->targets[i]).async(myBulk, std::string(filename), my_reqs[i].offset, my_reqs[i].len));

    }

    for (auto &r : responses) {
        size_t ret = r.wait();
        if (ret >= 0)
            bytes_moved += ret;
    }
    return bytes_moved;
}

ssize_t mochio_write(mochio_client_t client, const char *filename,
        const int64_t mem_count, const char * mem_addresses[], const uint64_t mem_sizes[],
        const int64_t file_count, const off_t file_starts[], const uint64_t file_sizes[])
{
    ssize_t ret;
    double write_time = ABT_get_wtime();
    client->statistics.client_write_calls++;

    ret = mochio_io(client, filename, MOCHIO_WRITE, mem_count, mem_addresses, mem_sizes,
            file_count, file_starts, file_sizes);

    write_time = ABT_get_wtime() - write_time;
    client->statistics.client_write_time += write_time;

    return ret;
}

ssize_t mochio_read(mochio_client_t client, const char *filename,
        const int64_t mem_count, const char *mem_addresses[], const uint64_t mem_sizes[],
        const int64_t file_count, const off_t file_starts[], const uint64_t file_sizes[])
{
    ssize_t ret;
    double read_time = ABT_get_wtime();
    client->statistics.client_read_calls++;

    ret = mochio_io(client, filename, MOCHIO_READ, mem_count, mem_addresses, mem_sizes,
            file_count, file_starts, file_sizes);

    read_time = ABT_get_wtime() - read_time;
    client->statistics.client_read_time += read_time;

    return ret;
}

int mochio_stat(mochio_client_t client, const char *filename, struct mochio_stats *stats)
{
    struct file_stats response = client->stat_op.on(client->targets[0])(std::string(filename));

    stats->blocksize = response.blocksize;
    stats->stripe_size = response.stripe_size;
    stats->stripe_count = response.stripe_count;

    /* also update client information.  This should probably be a 'map' keyed on file name */
    client->blocksize = response.blocksize;
    client->stripe_size = response.stripe_size;
    client->stripe_count = response.stripe_count;
    return(1);
}

int mochio_statistics(mochio_client_t client)
{
    int ret =0;
    for (auto target : client->targets) {
        auto s = client->statistics_op.on(target)();
        std::cout << "SERVER: ";
        io_stats(s).print_server();
    }
    std::cout << "CLIENT: ";
    client->statistics.print_client();
    return ret;
}

int mochio_flush(mochio_client_t client, const char *filename)
{
    int ret=0;
    for (auto target : client->targets)
        ret = client->flush_op.on(target)(std::string(filename));

    return ret;
}
