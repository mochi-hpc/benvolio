#include <thallium.hpp>

class file_stats {
    public:
        file_stats() : stripe_size(4096), stripe_count(1), blocksize(4096)  {};
        file_stats(int32_t s_size, int32_t s_count, int32_t b):
            stripe_size(s_size), stripe_count(s_count), blocksize(b) {};

        template<typename A> void serialize(A &ar) {
	    ar & status;
            ar & stripe_size;
            ar & stripe_count;
            ar & blocksize;
        }

	/* status */
	int status;
        /* parallel file system info */
        int32_t stripe_size;
        int32_t stripe_count;

        /* all file systems */
        int32_t  blocksize;
};
