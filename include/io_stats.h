#include <thallium.hpp>
class io_stats {
    public:
        io_stats() : write_rpc_calls(0), write_rpc_time(0.0),
	    read_rpc_calls(0), read_rpc_time(0.0),
	    getfd(0),
            server_write_calls(0), server_write_time(0.0),
	    bytes_written(0),
            client_write_expose(0.0), client_read_expose(0.0),
            server_read_calls(0), server_read_time(0.0), bytes_read(0),
	    write_bulk_time(0.0), write_bulk_xfers(0),
            read_bulk_time(0.0), read_bulk_xfers(0),
            write_expose(0.0), read_expose(0.0),
            write_response(0.0), read_response(0.0),
	    mutex_time(0.0),
            client_write_calls(0), client_write_time(0.0),
	    client_bytes_written(0),
	    client_read_calls(0), client_read_time(0.0),
	    client_bytes_read(0),
	    client_init_time(0.0),
            client_write_calc_striping_time(0.0),
            client_write_calc_request_time(0.0),
            client_write_post_request_time1(0.0),
            client_write_post_request_time2(0.0),
            client_write_wait_request_time(0.0)
            
    {};

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
        ar & write_bulk_xfers;
        ar & write_bulk_time;
        ar & read_bulk_xfers;
        ar & read_bulk_time;
        ar & getfd;
        ar & write_expose;
        ar & read_expose;
        ar & write_response;
        ar & read_response;
    }

    // i know it's bad form but it's a pain to write setter functions for these
    // so they are public
    /* - server things - */

    /* -- overall rpc stats -- */
    int write_rpc_calls;      // how many times the "write" rpc was called
    double write_rpc_time;    // how much time server spent in write rpc overall
    int read_rpc_calls;       // how many times "read" rpc was called
    double read_rpc_time;     // total time spent in read rpc

    /* -- file system stats -- */
    double getfd;             // time spent opening file or pulling fd out of data structure
    int64_t server_write_calls;   // how many write calls server made to process rpc
    double server_write_time; // time spent in write calls
    int64_t bytes_written;    // bytes written to storage
    int server_read_calls;    // how many read calls server made to process rpc
    double server_read_time;  // time spent in read calls
    int64_t bytes_read;       // bytes read from storage

    /* -- networking stats -- */
    double write_bulk_time;   // time spent in bulk xfer
    int64_t write_bulk_xfers;  // count of bulk xfers
    double read_bulk_time;   // time spent in bulk xfer
    int64_t read_bulk_xfers;  // count of bulk xfers
    double write_expose;     // time to register memory (get)
    double read_expose;     // time to register memory  (put)
    double write_response;   // time responding to client
    double read_response;   // time responding to client


    /* -- other stats -- */
    double mutex_time;        // time spent acquiring mutexes

    /* - client things - */
    int client_write_calls;   // number of times "bv_write" called
    double client_write_time; // time client spent in "bv_write",
    int64_t client_bytes_written; // bytes sent to provider
    int client_read_calls;    // number of tiems "bv_read" called
    double client_read_time;  // time client spent in "bv_read
    int64_t client_bytes_read; // bytes recieved from provider
    double client_init_time; // how long does it take to set everything up
    double client_write_expose; // time spent registering memory before writing
    double client_read_expose;  // time spent registering memory before reading


    double client_write_calc_striping_time;
    double client_write_calc_request_time;
    double client_write_post_request_time1;
    double client_write_post_request_time2;
    double client_write_wait_request_time;


    io_stats  & operator += (const io_stats &rhs) {
	write_rpc_calls += rhs.write_rpc_calls;
	write_rpc_time += rhs.write_rpc_time;
	read_rpc_calls += rhs.read_rpc_calls;
	read_rpc_time += rhs.read_rpc_time;

	getfd += rhs.getfd;
	server_write_calls += rhs.server_write_calls;
	server_write_time += rhs.server_write_time;
	bytes_written += rhs.bytes_written;
	server_read_calls += rhs.server_read_calls;
	server_read_time += rhs.server_read_time;
	bytes_read += rhs.bytes_read;

	write_bulk_time += rhs.write_bulk_time;
	write_bulk_xfers += rhs.write_bulk_xfers;
	read_bulk_time += rhs.read_bulk_time;
	read_bulk_xfers += rhs.read_bulk_xfers;
	write_expose += rhs.write_expose;
	read_expose += rhs.read_expose;
	write_response += rhs.write_response;
	read_response += rhs.read_response;

	mutex_time += rhs.mutex_time;

	client_write_calls += rhs.client_write_calls;
	client_write_time += rhs.client_write_time;
	client_read_calls += rhs.client_read_calls;
	client_read_time += rhs.client_read_time;
	client_init_time += rhs.client_init_time;
	client_write_calc_striping_time += rhs.client_write_calc_striping_time;
	client_write_calc_request_time += rhs.client_write_calc_request_time;
	client_write_post_request_time1 += rhs.client_write_post_request_time1;
	client_write_post_request_time2 += rhs.client_write_post_request_time2;
	client_write_wait_request_time += rhs.client_write_wait_request_time;

	return *this;
    }
    void print_server() {
        std::cout << server_to_str() << std::endl;
    }

    const std::string server_to_str() {
        std::ostringstream output;
        output << "write_rpc_calls " << write_rpc_calls
            << " write_rpc_time " << write_rpc_time
            << " server_write_calls " << server_write_calls
            << " server_write_time " << server_write_time
            << " bytes_written " << bytes_written
            << " write_expose " << write_expose
            << " write_bulk_time " << write_bulk_time
            << " write_bulk_xfers " << write_bulk_xfers
            << " write_response " << write_response
            << " read_rpc_calls " << read_rpc_calls
            << " read_rpc_time " << read_rpc_time
            << " server_read_calls " << server_read_calls
            << " server_read_time " << server_read_time
            << " bytes_read " << bytes_read
            << " read_bulk_time " << read_bulk_time
            << " read_bulk_xfers " << read_bulk_xfers
            << " read_expose " << read_expose
            << " read_response " << read_response
            << " getfd " << getfd
            << " mutex_time " << mutex_time;
        return output.str();
    }

    void print_client() {
        std::cout << client_to_str() << std::endl;
    }

    const std::string client_to_str() {
        std::ostringstream output;
        output << "client_write_calls " << client_write_calls
            << " client_write_time " << client_write_time
	    << " client_bytes_written " << client_bytes_written
            << " client_write_expose_time " << client_write_expose
            << " client_read_calls " << client_read_calls
            << " client_read_time " << client_read_time
	    << " client_bytes_read " << client_bytes_read
            << " client_read_expose_time " << client_read_expose
            << " client_init_time " << client_init_time
            << " client_write_calc_striping_time " << client_write_calc_striping_time
            << " client_write_calc_request_time " << client_write_calc_request_time
            << " client_write_post_request_time1 " << client_write_post_request_time1
            << " client_write_post_request_time2 " << client_write_post_request_time2
            << " client_write_wait_request_time " << client_write_wait_request_time;

        return output.str();
    }

    void print(void) {
        print_server();
        print_client();
    }
};
