# The Benvolio I/O service

Benvolio's goal is to provide many of the tried and true ROMIO optimizations in
an I/O service framework.  Based on [mochi](https://press3.mcs.anl.gov/mochi/)
services, benvolio will provide efficient i/o transformations to both MPI and
non-MPI applications.

[ROMIO](https://press3.mcs.anl.gov/romio) is a great way to optimize synchronous
parallel I/O in MPI codes, like writing a checkpoint or reading initial
conditions.  What if you are not using MPI in your application?  What if your
I/O is not synchronous?  Consider benvolio.

## Getting Started

You're probably reading this on gitlab or from a checked out benvolio
repository, but just in case you are not, you can get the latest benvolio from
<https://xgitlab.cels.anl.gov/sds/benvolio>

### prerequisites

* GNU Autotools to build
* [thallium](https://xgitlab.cels.anl.gov/sds/thallium) C++ wrappers to Mercury, Margo, and Argobots
* [ssg](https://xgitlab.cels.anl.gov/sds/ssg) group management for server identification
* [abt-io](https://xgitlab.cels.anl.gov/sds/abt-io/) providing I/O bindings for Argobots
* For the provider, either MPI or PMIx to launch the service.

We find the [spack](https://spack.readthedocs.io/en/latest/) package manager
very helpful for building everything you need.  We have some additional
packages in the [sds-repo](https://xgitlab.cels.anl.gov/sds/sds-repo).  Once
you have both spack and our extra repo installed you can build all the benvolio
prerequisites with

    spack install mochi-thallium mochi-abt-io mochi-ssg+pmix
    spack load -r mochi-thallium mochi-abt-io mochi-ssg+pmix


### building

With all the dependencies in place, the rest is straightforward:

* generate the configure script with `autoreconf -fi`
* `configure  --prefix=...`
* `make all tests`


### Simple execution

There are two parts to benvolio: the _provider_ (sometimes called the _server_)
and the _client_ .  A more complicated example would run the provider on one or
more nodes and the client on one or more (possibly the same) node.  In this
case we are just going to run everything on one node.

#### Provider with PMIx

1. PMIx:  Benvolio can use [PMIx](https://pmix.org/).  Some
job schedulers like SLURM are PMIx-aware, but more likely you will have to use
the PMIx reference runtime (prrte).  Spack can install that for you (`spack
install prrte` but NOTE: you'll need the one line fix in
<https://github.com/spack/spack/pull/16530/files> if you are using spack-0.14.2
or older).  Start the PRRTE managers:

    $ prte --host localhost:8 &

2. benvolio providers:  use `prun` to launch multiple providers.  A benvolio
provider takes quite a few command line arguments.  Only the `-f` and `-p`
arguments are mandatory.
  * `-f statefile` : file name to save provider state.  Clients will load this to find the providers.
  * `-p protocol` : what kind of protocol to use.  A mercury protocol identifier.
  * `-b buffersize`: size of internal memory buffer
  * `-x xfersize`  : the internal memory buffer will be partitioned into 'xfersize' pieces for pipelined transfers
  * `-t threads`   : how many I/O threads to spawn.
  * `-s streams`    : how many margo streams (Argobot Execution Streams) to use


    $ prun -np 2 ./src/provider/bv-server.pmix -f bv.svc -p sm:  &

#### Provider with MPI

1. Launching with MPI depends a lot on your platform.  `aprun`, `jsrun`, or simply
`mpiexec` are all possible mechanisms to start an MPI job.  For simplicity I
will assume `mpiexec`.  Consult your site-specific documentation if there is a
more apropriate mechanism

2. The command line arguments for `bv-server` are the same no matter how you launch the providers.

    $ mpiexec -np 2 ./src/provider/bv-server.mpi -f bv.svc -p sm: &

#### Client

No matter which way you started your benvolio provider, client code runs the same way.

1. A simple benvolio test:  `tests/simple` runs through a few I/O patterns.  It
has one mandatory argument: the name of the provider statefile.  You can give
it an optional file name, or it will write to and read from a file called
'dummy' by default. If you see `Error: PMIx event notification registration
failed! [-31]` you can ignore that for now.  We're still investigating
( <https://xgitlab.cels.anl.gov/sds/benvolio/issues/2> )

   $ ./tests/simple bv.svc


2. cleanup.  the `bv_shutdown` tool is a simple client with only one job: ask
the provider to shut itself down.  If necessary, we stop the prrte daemons with the
'-terminate' command

    $ ./src/client/bv-shutdown bv.svc
    $ prun -terminate


### Internal statistics

Benvolio developers can generate a timing report with a call to
`bv_statistics()`.  That call will emit a line contaning timings and counters
for benvolio's internal operations.  If the `show_server` flag is set, a client
will also emit information from all benvolio providers.

Here's an example of the output:

    SERVER: write_rpc_calls 2 write_rpc_time 0.0026474 server_write_calls 5 server_write_time 0.00186205 bytes_written 15011 write_expose 0 write_bulk_time 4.02927e-05 write_bulk_xfers 2 write_response 4.36306e-05 read_rpc_calls 2 read_rpc_time 0.0026679 server_read_calls 7 server_read_time 0.00181675 bytes_read 15006 read_bulk_time 5.31673e-05 read_bulk_xfers 2 read_expose 0 read_response 4.41074e-05 getfd 0.00121784 mutex_time 9.53674e-07
    CLIENT: client_write_calls 2 client_write_time 0.00291276 client_bytes_written 15011 client_read_calls 2 client_read_time 0.0028851 client_bytes_read 15006 client_init_time 0.0610526

In the above sample output, one can subtract (server) `write_rpc_time` from
(client) `client_write_time` to compute the RPC overhead.  Typically this
number will be quite small but could be large if for some reason the server was
unable to handle client requests efficiently (maybe requires more margo
threads?) or clients and servers were exchanging very complicated data
structures.

Benvolio is a mochi service, and as such makes heavy use of margo and mercury.
By setting the environment variable `MARGO_ENABLE_DIAGNOSTICS=1` benvolio will
also include additional information about this software abstraction layer.

```
# Margo diagnostics
#Addr Hash and Address Name: 18446744029030521930,ofi+verbs;ofi_rxm://10.41.20.24:56684
# Fri Jul 24 17:59:15 2020

# Function Name, Average Time Per Call, Cumulative Time, Highwatermark, Lowwatermark, Call Count
trigger_elapsed,0.000001399,0.000041962,0.000000238,0.000013590,30
progress_elapsed_zero_timeout,0.000002861,0.000002861,0.000002861,0.000002861,1
progress_elapsed_nonzero_timeout,0.007764083,0.100933075,0.000066280,0.090931177,13
bulk_create_elapsed,0.000027227,0.000136137,0.000003099,0.000055313,5
```

For more informaiton about Margo-level informatio, including profiling and the
immensely informative "breadcrumb" feature, please refer to [Margo
Instrumentation](https://xgitlab.cels.anl.gov/sds/margo/blob/master/doc/instrumentation.md)
