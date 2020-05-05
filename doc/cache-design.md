# Design for a Caching and Buffering layer in Benvolio

[Benvolio][benvolio-web] is an I/O midleware service: it relays I/O requests to/from an
application and a storage system.  We think of Benvolio as "ROMIO as a service"
-- it allows non-mpi applications to benefit from the kind of I/O
transformations and aggregations that ROMIO provides MPI-using applications.

[benvolio-web]: https://xgitlab.cels.anl.gov/sds/benvolio

## background and motivation

Benvolio does OK with large I/O requests.  We do not match straight MPI-IO in
an IOR benchmark, but come within a factor of 2.  Benvolio splits up large I/O
requests into smaller regions and then uses a pipeline approach to overlap
network and storage transfers.

The [E3SM I/O kernel][e3sm-io], on the other hand, provides a challenging workload.  E3SM,
through Parallel-NetCDF, generates thousands of 8 and 16 byte requests.
Parallel-NetCDF's multi-dataset optimization turns these subarray access to
over 400 multi-dimensional variables into a larger highly noncontiguous
collective MPI-IO call.  In aggregate, these many noncontiguous MPI-IO calls
look fairly contiguous.

[e3sm-io]: https://github.com/Parallel-NetCDF/E3SM-IO

The [ROMIO driver][romio-bv] for Benvolio looks a lot like the old PVFS driver.
Benvolio's read and write rpc calls take vectors of memory addresses/lengths
and file offsets/sizes.  ROMIO uses the old PVFS listio code to processes MPI
datatypes and file views into these memory and file descriptions.

[romio-bv]: https://github.com/roblatham00/mpich/tree/benvolio

While Benvolio appears to handle this workload without crashing, the
performance is terrible.  On Summit, 60 MPI clients writing to GPFS directly
achieve 282.2450 MiB/sec whereas 60 MPI clients writing to one benvolio process
see only 1.1475 MiB/sec

## Approaches

Two optimizations immediately come to mind:  operation coalescing and
buffering.  Coalescing is perhaps the more straightforward to implement, but
buffering will offer benefits to more cases.

### Coalescing

We know the overall collection of requests is contiguous: The gaps in one
client's request are filled in with requests from other clients.  We also can
measure the number of outstanding operations and see that benvolio has a pool
of close to 60,000 pending operations.

One approach might be to issue requests to or from storage as soon as they are
visible to benvolio until the backlog of outstanding requests hits a threshold.
At that point, benvolio could switch to "coalescing" mode.  After receiving an
I/O request but before issuing an I/O operation for that request, benvolio
would scan the list of pending operations looking for adjacent requests.


### Delegation and Buffering

In the specific case of the E3SM-IO kernel, coalescing seems like a clear win.
However, not all workloads end up with globally adjacent I/O requests.  An
approach like the ROMIO data sieving optimization provides additional coverage
over I/O requests with holes (perhaps due to block alignment padding or from
some nature of the application's data)

Northwestern has some experience in this area:  Arifa Nissar's
[I/O delegation and caching][nissar:caching] work showed that setting aside
some MPI processes allowed highly non-contiguous writes to behave as if they
were well-formed large contiguous
writes.  The delegated processes behaved like ROMIO aggregators would in the
collective I/O case, but did not require the same level of coordination and
synchronization that collective I/O would impose.

[nissar:caching]: http://users.eecs.northwestern.edu/~choudhar/Publications/NisLia08.pdf


#### Delegation

TODO: clarify this a lot

First we need a way to tell
a server which parts of a file it owns.  Consider ROMIO where file domains can
be block-aligned (GPFS) or group-cyclic (Lustre). 

 * Providers don't know about any files or file systems on startup.
 * benvolio has a `bv_declare` routine to trigger file creation and fast-fail any permission or path errors.
 * benvolio also has a `bv_stat` routine, mainly so clients can get the underlying blocksize, stripe count, and stripe size

Do we have enough information for both client and server to agree who owns which regions of a file?

Idea: if client erroneously sends data to the wrong provider, can provider
return an error? ("No, that's not my data.  you should have sent it to server
X")

Want something that works for both GPFS and Lustre


#### Client Driven vs Target Driven

Right now the clients fire of I/O requests as fast as possible, and benvolio
providers process the RPCs first come first served order.

Consider a "not yet" NACK:

* server has too much work queued up
* server presently working on one region of a file.  Jumping to a different region unlikely to perform well.
* clients in aggregate have more memory
* Server tells client how long to wait before retrying.  Maybe delay increases as pending operations increases
* how often does NACK happen?  consider random reject, higher likelihood of reject as pending operations increases
* should server remember rejected client requests?   completely forget easier to implement but retaining information about who has what could mean we can make smarter requests
* can server negative response be a request?  "I cannot handle X. Do you have Y?"


#### Data Structures

* mapping between byte in file and buffer page
* full blocks: dispatch to storage immediately
* phantom entry: in progress I/O (large block, long transfer)
* small i/o: allocate buffer page and track
