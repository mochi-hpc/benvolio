#include <thallium.hpp>
#include <romio-svc.h>
#include <utility>
#include <vector>

namespace tl = thallium;

struct romio_client {
    tl::engine *engine;
    // won't be a single server but rather a collection of them (ssg group?)
    tl::endpoint server;
    tl::remote_procedure io_op;
};

typedef enum {
    ROMIO_READ,
    ROMIO_WRITE
} op_kind;

int romio_init(char * protocol, char * provider, romio_client_t  client )
{
    /* This is very c-like.  probably needs some C++ RAII thing here */
    client->engine = new tl::engine(protocol, THALLIUM_CLIENT_MODE);
    client->server = client->engine->lookup(provider);

    client->io_op = client->engine->define("io");

    return 0;
}

int romio_finalize(romio_client_t client)
{
    delete client->engine;
    return 0;
}

// use bulk transfer for the memory description
// the lcoations in file we will just send over in a list
// - could compress the file locations: they are likely to compress quite well
// - not doing a whole lot of other data manipulation on the client

static size_t romio_io(romio_client_t client, char *filename, op_kind op,
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
    return (client->io_op.on(client->server)(myBulk));
}

size_t romio_write(romio_client_t client, char *filename, int64_t iovcnt, const struct iovec iov[],
        int64_t file_count, const off_t file_starts[], uint64_t file_sizes[])
{
    return (romio_io(client, filename, ROMIO_WRITE, iovcnt, iov, file_count, file_starts, file_sizes));
}

size_t romio_read(romio_client_t client, char *filename, int64_t iovcnt, const struct iovec iov[],
        int64_t file_count, const off_t file_starts[], uint64_t file_sizes[])
{
    return (romio_io(client, filename, ROMIO_READ, iovcnt, iov, file_count, file_starts, file_sizes));
}


