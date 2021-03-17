#include <thallium.hpp>


enum {
    BV_BLOCK_ALIGNED,
    BV_GROUP_CYCLIC
};


class file_stats {
    public:
        file_stats() : stripe_size(4096), stripe_count(1), blocksize(4096), distribution_kind(BV_BLOCK_ALIGNED) {};
        file_stats(int32_t s_size, int32_t s_count, int32_t b, int32_t kind):
            stripe_size(s_size), stripe_count(s_count), blocksize(b), distribution_kind(kind) {};

        template<typename A> void serialize(A &ar) {
	    ar & status;
            ar & stripe_size;
            ar & stripe_count;
            ar & blocksize;
            ar & distribution_kind;
        }

	/* status */
	int status;
        /* parallel file system info */
        int32_t stripe_size;
        int32_t stripe_count;

        /* all file systems */
        int32_t  blocksize;

        /* data redistribution scheme */
        int32_t distribution_kind;
};
