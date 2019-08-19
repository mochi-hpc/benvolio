#include <thallium.hpp>

class file_stats {
    public:
        file_stats() : stripe_size(4096), stripe_count(1), blocksize(4096)  {};

        template<typename A> void serialize(A &ar) {
            ar & stripe_size;
            ar & stripe_count;
            ar & blocksize;
        }

        /* parallel file system info */
        int32_t stripe_size;
        int32_t stripe_count;

        /* all file systems */
        int32_t  blocksize;
};
