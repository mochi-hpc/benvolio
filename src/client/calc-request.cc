/* extracted from MPICH adio/ad_lustre/ad_lustre_aggregate.c with the following differences: 
 * - providers are setaside processes, so we don't need to map back and forth
 *   from "nth aggregator" to "mpi rank"
 *
 */
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <vector>

#include "common.h"
#include "access.h"
#include "sys/uio.h"
#include "calc-request.h"


/* taken from ROMIO's ADIOI_LUSTRE_Get_striping_info, but modified to take
 * parameters explicity instead of through the fd->hints interface:
 * - stripe_size: [IN] how big a stripe on lustre is
 * - stripe_count: [IN] how many OST a file is spread across
 * - server_count: [INOUT] how many MOCHIO providers there are, modified upon
 *   return to indicate how many MOCHI providers we will use (in case where
 *   stripe count is less than number of providers)
 *   'oversubscribe_factor':  how many MOCHIO targets we want talking to each
 *       lustre OST.  Equivalent ROMIO default is 1 */

void compute_striping_info(int stripe_size, int stripe_count, int *server_count, int oversubscribe_factor)
{
    int CO;
    int avail_cb_nodes, divisor, nprocs_for_coll = *server_count;

    /* Calculate the available number of I/O clients */
    /* for collective read,
     * if "CO" clients access the same OST simultaneously,
     * the OST disk seek time would be much. So, to avoid this,
     * it might be better if 1 client only accesses 1 OST.
     */
    CO = oversubscribe_factor; /*XXX: maybe there are other better way for collective read */

    /* Calculate how many IO clients we need */
    /* Algorithm courtesy Pascal Deveze (pascal.deveze@bull.net) */
    /* To avoid extent lock conflicts,
     * avail_cb_nodes should either
     *  - be a multiple of stripe_count,
     *  - or divide stripe_count exactly
     * so that each OST is accessed by a maximum of CO constant clients. */
    if (nprocs_for_coll >= stripe_count)
        /* avail_cb_nodes should be a multiple of stripe_count and the number
         * of procs per OST should be limited to the minimum between
         * nprocs_for_coll/stripe_count and CO
         *
         * e.g. if stripe_count=20, nprocs_for_coll=42 and CO=3 then
         * avail_cb_nodes should be equal to 40 */
        avail_cb_nodes = stripe_count * MIN(nprocs_for_coll / stripe_count, CO);
    else {
        /* nprocs_for_coll is less than stripe_count */
        /* avail_cb_nodes should divide stripe_count */
        /* e.g. if stripe_count=60 and nprocs_for_coll=8 then
         * avail_cb_nodes should be egal to 6 */
        /* This could be done with :
         * while (stripe_count % avail_cb_nodes != 0) avail_cb_nodes--;
         * but this can be optimized for large values of nprocs_for_coll and
         * stripe_count */
        divisor = 2;
        avail_cb_nodes = 1;
        /* try to divise */
        while (stripe_count >= divisor * divisor) {
            if ((stripe_count % divisor) == 0) {
                if (stripe_count / divisor <= nprocs_for_coll) {
                    /* The value is found ! */
                    avail_cb_nodes = stripe_count / divisor;
                    break;
                }
                /* if divisor is less than nprocs_for_coll, divisor is a
                 * solution, but it is not sure that it is the best one */
                else if (divisor <= nprocs_for_coll)
                    avail_cb_nodes = divisor;
            }
            divisor++;
        }
    }

    *server_count = avail_cb_nodes;
}
int calc_aggregator(off_t off, uint64_t * len,
        int stripe_size, int server_count)
{
    int rank_index, rank;
    int64_t avail_bytes;
    int avail_cb_nodes = server_count;

    /* Produce the stripe-contiguous pattern for Lustre */
    rank_index = (int) ((off / stripe_size) % avail_cb_nodes);

    avail_bytes = (off / (int64_t) stripe_size + 1) * (int64_t) stripe_size - off;
    if (avail_bytes < *len) {
        /* this proc only has part of the requested contig. region */
        *len = avail_bytes;
    }
    /* unlike ROMIO, the providers are fixed and dedicated.  We don't need to
     * turn "nth aggregator" into an MPI rank.  It's sufficient to know "nth
     * I/O provider"  */
    rank = rank_index;

    return rank;
}

/* Inspired by ROMIO's 'ADIOI_LUSTRE_Calc_my_req', but completely rewritten.
 * This version is a bit more complicated in that we are not only trying to
 * figure out where a file request goes, but also trying to match each piece of
 * the memory description with each part of the file description.
 * remember:
 * - vector<struct access> is "number of targets" big
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
int calc_requests(int iovec_count, const struct iovec *memvec,
        int file_count, const off_t *file_starts, const uint64_t *file_sizes,
        int stripe_size, int targets_used,
        std::vector<struct access> & my_reqs)
{
    // which element of the memory/file description list we are working on now
    int memblk=0, fileblk=0;
    // how much of a memory/file block is left to process
    u_int64_t memblk_used=0, fileblk_used=0;

    while (memblk < iovec_count && fileblk < file_count) {
        uint64_t len;
        int target;
        char *addr;
        off_t offset;

        // for each file block we might split it into smaller pieces based on
        // - which target handles this offset and how much of the data goes there
        // - a memory block that might be smaller still

        // '*_used' handles partial progress through a block
        len = file_sizes[fileblk] - fileblk_used;
        target = calc_aggregator(file_starts[fileblk]+fileblk_used, &(len), stripe_size, targets_used);

        // may have to further reduce 'len' if the corresponding memory block
        // is smaller
        len = MIN(memvec[memblk].iov_len - memblk_used, len);
        addr = (char *)memvec[memblk].iov_base + memblk_used;
        offset = file_starts[fileblk] + fileblk_used;

        my_reqs[target].mem_vec.push_back(std::make_pair(addr, len));
        my_reqs[target].offset.push_back(offset);
        my_reqs[target].len.push_back(len);

        /* a bunch of bookeeping for the next time through: do we need to work
         * on the next memory or file block? how much of current block is left
         * and how far into it are we? */

        memblk_used += len;
        fileblk_used += len;
        if (memblk_used >= memvec[memblk].iov_len) {
            memblk++;
            memblk_used = 0;
        }
        if ((int64_t)fileblk_used >= file_sizes[fileblk]) {
            fileblk++;
            fileblk_used = 0;
        }
    }
    return 0;
}
