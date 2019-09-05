#include <vector>
#include "access.h"
/** taken from ROMIO's ADIOI_LUSTRE_Get_striping_info, but modified to take
 * parameters explicity instead of through the fd->hints interface:
 * - stripe_size: [IN] how big a stripe on lustre is
 * - stripe_count: [IN] how many OST a file is spread across
 * - server_count: [INOUT] how many MOCHIO providers there are, modified upon
 *   return to indicate how many MOCHI providers we will use (in case where
 *   stripe count is less than number of providers)
 *   'oversubscribe_factor':  how many MOCHIO targets we want talking to each
 *       lustre OST.  Equivalent ROMIO default is 1 */
void compute_striping_info(int stripe_size, int stripe_count, int *server_count, int oversubscribe_factor);

/* Inspired by ROMIO's 'ADIOI_LUSTRE_Calc_my_req', but completely rewritten.
 * This version is a bit more complicated in that we are not only trying to
 * figure out where a file request goes, but also trying to match each piece of
 * the memory description with each part of the file description.
 * remember:
 * - vector<io_access> is "number of targets" big
 * - we generate offset, length, and pointer vectors for each target in the my_reqs vector.
 *
 * iovec_count: [IN] how many memory requests
 * memvec:      [IN] array of memory descriptions (address/length)
 * file_count:  [IN] how many file requests
 * file_starts  [IN] array of starting offsets
 * file_sizes   [IN] array of file access lengths
 * stripe_size  [IN] how big a stripe the remote end's file system will use for this file
 * targets_used [IN] 'compute_striping_info' told us this many targets will meet this request
 * my_reqs      [OUT] a collection holding the parametes we will need to issue to each I/O target
 *
 * returns: 0 on sucess, non-zero on failure
 *
 **/
int calc_requests(int mem_count, const char *mem_addresses[], const uint64_t mem_sizes[],
        int file_count, const off_t *file_starts, const uint64_t *file_sizes,
        int stripe_size, int targets_used,
        std::vector<io_access> & my_reqs);
