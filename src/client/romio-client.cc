#include <thallium.hpp>
#include <romio-svc.h>
#include <utility>
#include <vector>
#include <ssg.h>
#include <thallium/serialization/stl/string.hpp>

#include <assert.h>
namespace tl = thallium;

struct romio_client {
    tl::engine *engine;
    std::vector<tl::provider_handle> targets;
    tl::remote_procedure io_op;
    tl::remote_procedure stat_op;
    ssg_group_id_t gid;     // attaches to this group; not a member

    ssize_t blocksize=1024*4; // TODO: make more dynamic
};

typedef enum {
    ROMIO_READ,
    ROMIO_WRITE
} io_kind;

romio_client_t romio_init(char * protocol, char * cfg_file)
{
    char *addr_str;
    int ret, i, nr_targets;
    struct romio_client * client = (struct romio_client *)calloc(1,sizeof(*client));
    /* This is very c-like.  probably needs some C++ RAII thing here */
    client->engine = new tl::engine(protocol, THALLIUM_CLIENT_MODE);

    ssg_init(client->engine->get_margo_instance());

    ret = ssg_group_id_load(cfg_file, &(client->gid));
    assert (ret == SSG_SUCCESS);
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

    client->io_op = client->engine->define("io");
    client->stat_op = client->engine->define("stat");

    free(addr_str);
    return client;
}

/* need to patch up the API i think: we don't know what servers to talk to.  Or
 * do you talk to one and then that provider informs the others? */
int romio_setchunk(char *file, ssize_t nbytes)
{
}

int romio_finalize(romio_client_t client)
{
    delete client->engine;
    free(client);
    return 0;
}

// use bulk transfer for the memory description
// the lcoations in file we will just send over in a list
// - could compress the file locations: they are likely to compress quite well
// - not doing a whole lot of other data manipulation on the client

static size_t romio_io(romio_client_t client, char *filename, io_kind op,
        int64_t iovcnt, const struct iovec iovec_iov[],
        int64_t file_count, const off_t file_starts[], uint64_t file_sizes[])
{
    std::vector<std::pair<void *, std::size_t>> mem_vec;
    for (int i=0; i< iovcnt; i++)
        mem_vec.push_back(std::make_pair(iovec_iov[i].iov_base, iovec_iov[i].iov_len));

    std::vector<std::pair<off_t, uint64_t>> file_vec;
    for (int i=0; i< file_count; i++)
        file_vec.push_back(std::make_pair(file_starts[i], file_sizes[i]));

    tl::bulk myBulk;
    if (op == ROMIO_WRITE) {
        myBulk = client->engine->expose(mem_vec, tl::bulk_mode::read_only);
    } else {
        myBulk = client->engine->expose(mem_vec, tl::bulk_mode::read_write);
    }
    return (client->io_op.on(client->targets[0])(myBulk));
}

ssize_t romio_write(romio_client_t client, char *filename, int64_t iovcnt, const struct iovec iov[],
        int64_t file_count, const off_t file_starts[], uint64_t file_sizes[])
{
    return (romio_io(client, filename, ROMIO_WRITE, iovcnt, iov, file_count, file_starts, file_sizes));
}

ssize_t romio_read(romio_client_t client, char *filename, int64_t iovcnt, const struct iovec iov[],
        int64_t file_count, const off_t file_starts[], uint64_t file_sizes[])
{
    return (romio_io(client, filename, ROMIO_READ, iovcnt, iov, file_count, file_starts, file_sizes));
}

int romio_stat(romio_client_t client, char *filename, struct romio_stats *stats)
{
    stats->blocksize = client->stat_op.on(client->targets[0])(std::string(filename) );
    return(1);
}
