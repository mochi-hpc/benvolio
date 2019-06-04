#include <thallium.hpp>
class io_stats {
    public:
        io_stats() : write_rpc_calls(0), write_rpc_time(0.0),
            server_write_calls(0), bytes_written(0), read_rpc_calls(0),
            server_read_calls(0), server_read_time(0.0), bytes_read(0),
            mutex_time(0.0), client_write_calls(0), client_read_calls(0),
            client_read_time(0.0) {};

    template<typename A> void serialize(A &ar) {
        ar & write_rpc_calls;
        ar & write_rpc_time;
        ar & server_write_calls;
        ar & server_write_time;
        ar & bytes_written;
        ar & read_rpc_calls;
        ar & read_rpc_time;
        ar & server_read_calls;
        ar & server_read_time;
        ar & bytes_read;
        ar & mutex_time;
    }

    // i know it's bad form but it's a pain to write setter functions for these
    // so they are public
    /* server things */
    int write_rpc_calls;      // how many times the "write" rpc was called
    double write_rpc_time;    // how much time server spent in write rpc overall
    int server_write_calls;   // how many write calls server made to process rpc
    double server_write_time; // time spent in write calls
    int64_t bytes_written;    // bytes written to storage
    int read_rpc_calls;       // how many times "read" rpc was called
    double read_rpc_time;     // total time spent in read rpc
    int server_read_calls;    // how many read calls server made to process rpc
    double server_read_time;  // time spent in read calls
    int64_t bytes_read;       // bytes read from storage
    double mutex_time;        // time spent acquiring mutexes
    /* client things */
    int client_write_calls;   // number of times "mochio_write" called
    double client_write_time; // time client spent in "mochio_write",
    int client_read_calls;    // number of tiems "mochio_read" called
    double client_read_time;  // time client spent in "mochio_read

    io_stats & operator = (const io_stats &rhs);

    void print(void) {
        std::cout << "SERVER: "
            << "write_rpc_calls " << write_rpc_calls
            << " write_rpc_time " << write_rpc_time
            << " server_write_calls " << server_write_calls
            << " server_write_time " << server_write_time
            << " bytes_written " << bytes_written
            << " read_rpc_calls " << read_rpc_calls
            << " read_rpc_time " << read_rpc_time
            << " server_read_calls " << server_read_calls
            << " server_read_time " << server_read_time
            << " bytes_read " << bytes_read
            << " mutex_time " << mutex_time << std::endl;
        std::cout << "CLIENT: "
            << " client_write_calls " << client_write_calls
            << " client_write_time " << client_write_time
            << " client_read_calls " << client_read_calls
            << " client_read_time " << client_read_time << std::endl;
    }
};

io_stats & io_stats::operator = (const io_stats &rhs)
{
    // overwriting basic types so no need to worry about self-assignment
    /* server things */
    write_rpc_calls = rhs.write_rpc_calls;
    write_rpc_time = rhs.write_rpc_time;
    server_write_calls = rhs.server_write_calls;
    server_write_time = rhs.server_write_time;
    bytes_written = rhs.bytes_written;
    read_rpc_calls = rhs.read_rpc_calls;
    read_rpc_time = rhs.read_rpc_time;
    server_read_calls = rhs.server_read_calls;
    server_read_time = rhs.server_read_time;
    bytes_read = rhs.bytes_read;
    mutex_time = rhs.mutex_time;
    /* client things */
    client_write_calls = rhs.client_write_calls;
    client_write_time = rhs.client_write_time;
    client_read_calls = rhs.client_read_calls;
    client_read_time = rhs.client_read_time;

    return *this;
}

io_stats  operator + (const io_stats & lhs, const io_stats &rhs)
{
    io_stats sum;
    sum.write_rpc_calls = lhs.write_rpc_calls + rhs.write_rpc_calls;
    sum.write_rpc_time = lhs.write_rpc_time + rhs.write_rpc_time;
    sum.server_write_calls = lhs.server_write_calls + rhs.server_write_calls;
    sum.server_write_time = lhs.server_write_time + rhs.server_write_time;
    sum.bytes_written = lhs.bytes_written  + rhs.bytes_written;
    sum.read_rpc_calls = lhs.read_rpc_calls  + rhs.read_rpc_calls;
    sum.read_rpc_time = lhs.read_rpc_time  + rhs.read_rpc_time;
    sum.server_read_calls = lhs.server_read_calls  + rhs.server_read_calls;
    sum.server_read_time = lhs.server_read_time  + rhs.server_read_time;
    sum.bytes_read = lhs.bytes_read  + rhs.bytes_read;
    sum.mutex_time = lhs.mutex_time  + rhs.mutex_time;
    /* client things */
    sum.client_write_calls = lhs.client_write_calls  + rhs.client_write_calls;
    sum.client_write_time = lhs.client_write_time  + rhs.client_write_time;
    sum.client_read_calls = lhs.client_read_calls  + rhs.client_read_calls;
    sum.client_read_time = lhs.client_read_time  + rhs.client_read_time;

    return sum;
}
