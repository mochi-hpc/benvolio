#include <romio-svc.h>
#include <thallium.hpp>
#include <utility>
#include <vector>
#include <ssg.h>
#include <mpi.h>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <assert.h>
namespace tl = thallium;

#define MAX_PROTO_LEN 24

struct romio_client {
    tl::engine *engine;
    std::vector<tl::provider_handle> targets;
    char proto[MAX_PROTO_LEN];
    tl::remote_procedure read_op;
    tl::remote_procedure write_op;
    tl::remote_procedure stat_op;
    tl::remote_procedure delete_op;
    tl::remote_procedure flush_op;
    ssg_group_id_t gid;     // attaches to this group; not a member

    ssize_t blocksize=1024*4; // TODO: make more dynamic
};

typedef enum {
    ROMIO_READ,
    ROMIO_WRITE
} io_kind;

int  set_proto_from_addr(romio_client_t client, char *addr_str)
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
romio_client_t romio_init(MPI_Comm comm, const char * cfg_file)
{
    char *addr_str;
    int rank;
    int ret, i, nr_targets;
    struct romio_client * client = (struct romio_client *)calloc(1,sizeof(*client));
    char *ssg_group_buf;


    /* scalable read-and-broadcast of group information: only one process reads
     * cfg from file system.  These routines can all be called before ssg_init */
    MPI_Comm_rank(comm, &rank);
    size_t ssg_serialize_size;
    ssg_group_buf = (char *) malloc(1024); // how big do these get?
    if (rank == 0) {
        ret = ssg_group_id_load(cfg_file, &(client->gid));
        assert (ret == SSG_SUCCESS);
        ssg_group_id_serialize(client->gid, &ssg_group_buf, &ssg_serialize_size);
    }
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

    struct romio_stats stats;
    // TODO: might want to be able to set distribution on a per-file basis
    romio_stat(client, "/dev/null", &stats);
    client->blocksize = stats.blocksize;

    free(addr_str);
    return client;
}

/* need to patch up the API i think: we don't know what servers to talk to.  Or
 * do you talk to one and then that provider informs the others? */
int romio_setchunk(const char *file, ssize_t nbytes)
{
}

int romio_delete(romio_client_t client, const char *file)
{
    return client->delete_op.on(client->targets[0])(std::string(file) );
}
int romio_finalize(romio_client_t client)
{
    ssg_group_detach(client->gid);
    ssg_finalize();
    delete client->engine;
    free(client);
    return 0;
}

// use bulk transfer for the memory description
// the lcoations in file we will just send over in a list
// - could compress the file locations: they are likely to compress quite well
// - not doing a whole lot of other data manipulation on the client

static size_t romio_io(romio_client_t client, const char *filename, io_kind op,
        int64_t iovcnt, const struct iovec iovec_iov[],
        int64_t file_count, const off_t file_starts[], uint64_t file_sizes[])
{
    std::vector<std::pair<void *, std::size_t>> mem_vec;
    for (int i=0; i< iovcnt; i++)
        mem_vec.push_back(std::make_pair(iovec_iov[i].iov_base, iovec_iov[i].iov_len));

    std::vector<off_t> offset_vec;
    std::vector<uint64_t> size_vec;
    for (int i=0; i< file_count; i++) {
        offset_vec.push_back(file_starts[i]);
        size_vec.push_back(file_sizes[i]);
    }

    tl::bulk myBulk;
    if (op == ROMIO_WRITE) {
        myBulk = client->engine->expose(mem_vec, tl::bulk_mode::read_only);
        return (client->write_op.on(client->targets[0])(myBulk, std::string(filename), offset_vec, size_vec));
    } else {
        myBulk = client->engine->expose(mem_vec, tl::bulk_mode::read_write);
        return (client->read_op.on(client->targets[0])(myBulk, std::string(filename), offset_vec, size_vec));
    }
}

ssize_t romio_write(romio_client_t client, const char *filename, int64_t iovcnt, const struct iovec iov[],
        int64_t file_count, const off_t file_starts[], uint64_t file_sizes[])
{
    return (romio_io(client, filename, ROMIO_WRITE, iovcnt, iov, file_count, file_starts, file_sizes));
}

ssize_t romio_read(romio_client_t client, const char *filename, int64_t iovcnt, const struct iovec iov[],
        int64_t file_count, const off_t file_starts[], uint64_t file_sizes[])
{
    return (romio_io(client, filename, ROMIO_READ, iovcnt, iov, file_count, file_starts, file_sizes));
}

int romio_stat(romio_client_t client, const char *filename, struct romio_stats *stats)
{
    stats->blocksize = client->stat_op.on(client->targets[0])(std::string(filename) );
    return(1);
}

int romio_flush(romio_client_t client, const char *filename)
{
    int ret=0;
    for (auto target : client->targets)
        ret = client->flush_op.on(target)(std::string(filename));

    return ret;
}
