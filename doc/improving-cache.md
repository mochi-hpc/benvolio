# Rethinking the benvolio provider-side cache

Benvolio has a caching layer to optimize small I/O requests. The E3SM I/O
pattern, for example, needs something to coalesce and transform a series of
many small writes into something more file system friendly.  In spring 2021 the
cache behaved correctly but as we added more clients and providers we could
only get about half the bandwidth of collective I/O straight to GPFS.

## Background

The benvolio C api looks like this:

```
    ssize_t bv_write(bv_client_t client,
        const char *file,
        const int64_t mem_count,
        const char *mem_addresses[],
        const uint64_t mem_sizes[],
        const int64_t file_count,
        const off_t file_starts[],
        const uint64_t file_sizes[]);
```

The client processes these memory and file size/offset lists so that they end
up on the "right" provider (Lustre and GPFS dictate what "right" means).  It is
sufficient to understand that the list as well as elements of that list can be
further split up across providers.


The client fires off a non-blocking request to each provider.  RPC looks like this

```
    ssize_t process_write(const tl::request& req, tl::bulk &client_bulk,
        const std::string &file,
	const std::vector<off_t> &file_starts, const std::vector<int64_t> &file_sizes,
	int stripe_count, int stripe_size)
```

The noncontiguous in memory part is conveyed in the bulk handle (because I
incorrectly thought it would go fast on networks).

The stripe information is not used:  at one time we thought it might be good to
be able to double-check that a provider got the data it was supposed to get,
but right now providers just trust the clients are sending the appropriate
regions.


## Cache as it stands

Start with `process_write`
(https://github.com/mochi-hpc/benvolio/blob/main/src/provider/bv-provider.cc#L625)
.  The provider "reigsters" a cache page
(https://github.com/mochi-hpc/benvolio/blob/main/src/provider/bv-provider.cc#L689)
.  This registration step either modifies an existing cache page or creates a
new cache page

There is a `Cache_info` data structure holding general cache information
(https://github.com/mochi-hpc/benvolio/blob/main/src/provider/bv-cache.h#L81) .
On a per-file basis a map of offsets to cache blocks is maintained in a
`Cache_file_info` data structure:
(https://github.com/mochi-hpc/benvolio/blob/main/src/provider/bv-cache.h#L118 )

There are two conditions that trigger writing cached data to a file:
The first condition is when a client calls the `bv_flush` or
`bv_close` RPC explicitly ( see the register/write\_back/deregister/remove
sequence at
https://github.com/mochi-hpc/benvolio/blob/main/src/provider/bv-provider.cc#L1138
) . The second condition is when a file has not been accessed for a certain
period. A cleaner thread constantly monitors the timestamps for file accesses.
When a file access timestamp is old enough, all cache pages of the file are
synchronized with the file systems. (see the `cache_resource_manager`
https://github.com/mochi-hpc/benvolio/blob/main/src/provider/bv-provider.cc#L1138
)

Any time we touch the cache data structure we grab a Thallium mutex (thin
wrapper around Argobots mutex).  The `Cache_info` data structure has one and
each `Cache_file_info` object has one as well.

### Timing breakdown

I've collected data from Summit.  With 40 nodes I ran 1 provider per node (40
providers total) and 32 clients per node (1280 clients).  E3SM-IO is a
parallel-netcdf code, so in the end we have a very long list of offset-length
pairs from every client.  Each provider sees about 60,000 `bv_write` RPCs.
Sums and averages can wash out some information so let's look at just one
provider.

Looking at cumulative time, and picking server 0 here's the breakdown
of cumulative timing:

`write_rpc_calls`:  62205 (just for context)

| Routine  |  Time (cumulative seconds) | Comment |
| ---      | --- |  ---  |
|`write_rpc_time` | 831.237 |    total time spent in the `process_write` rpc handler |
|`write_bulk_time`| 8.49236 |   RMA transfers from client to host |
|`write_response` | 14.6973  |   sending response back to client |
|`acquire_pool_time` | 57.9879|  time spent waiting for `margo_buffer_pool` # to provide a pre-registered region |
|`total_cache_time` | 724.427021 | anything cache related.  Does also include I/O time: benvolio timings did not capture I/O write time

## Ideas for improvement

Well, yeah, this is where I hope readers can suggest something better.
- Some kind of lock-free data structure that accepts writes immediately?
- An intermediate data structure that can quickly accept updates?
- An array of locks so that we can make updates on a more granular basis?
