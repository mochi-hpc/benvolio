
# Service-oriented improvements for ROMIO

(copied from
https://confluence.exascaleproject.org/display/STDM12/ROMIO+for+exascale+ideas
for anyone without an ECP confluence acct)

Short summary:  set up an I/O service to  process I/O requests.  This I/O
service acts in many ways like a ROMIO I/O aggregator, but is not part of the
communicator and can absorb writes without perturbing the rest of the
communication.  Related work

## Background
Consider the "tcp cork" optimization, where a client can issue a bunch of
network operations after "corking" the network, then fire them all off by
uncorking.  This decoupling of description from execution optimization helpful,
recurs in several places.

Work would draw from several prior efforts

* Northwestern Delegation work
* IOFSL I/O forwarding
* Posix extensions proposal (lazy i/o, listio
* ROMIO data sieving
* PVFS listio
* "Active Buffering with Threads", Xiaosong Ma, UIUC: exciting research we
could not implement in ROMIO because I/O aggregators only participate in
collective I/O.  Independent operations would bypass the buffers, sadly.  If in
this approach all I/O goes through a delegate then we sidesetep this drawback.

## Rough design sketch

Observe that RDMA allows us to separate control message from data. 

*  Initiator sends description of I/O to Target,
*  Target queues up request
*  Target services operations in queue
    * as queue gets larger, more opportunites to scan outstanding requests and coalesce, rearange, or transform
    * .. but if queue is empty, just service request

Will we want to compress control messages?  We know they will compress with
something like 90% effeciency

## RPC

Definitely needs:

* write(filename, offset, length)
* read(filename, offset, length)

Still working out details for these:

* stat
* cork/uncork
* synchronize
* flush

We discussed an idea of handing off permission/responsibility for some
region(s) of a file to a given provider, so that the provider can
cache/buffer/whatever however it wants. This fits in with the sync/flush idea
above, allowing the client to programmatically control when the provider is
forced to communicate with the backend FS (e.g., Lustre).

## Statistics

Will probably be helpful to keep track of and report on some metrics

* queue depth
* time in queue
