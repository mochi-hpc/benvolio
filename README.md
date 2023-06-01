# The Benvolio I/O service

Benvolio's goal is to provide many of the tried and true ROMIO optimizations in
an I/O service framework.  Based on [mochi](https://wordpress.cels.anl.gov/mochi/)
services, benvolio will provide efficient i/o transformations to both MPI and
non-MPI applications.

[ROMIO](https://wordpress.cels.anl.gov/romio) is a great way to optimize synchronous
parallel I/O in MPI codes, like writing a checkpoint or reading initial
conditions.  What if you are not using MPI in your application?  What if your
I/O is not synchronous?  Consider benvolio.

## Getting Started

You're probably reading this on github or from a checked out benvolio
repository, but just in case you are not, you can get the latest benvolio from
<https://github.com/mochi-hpc/benvolio>

### prerequisites

* GNU Autotools to build
* [thallium](https://github.com/mochi-hpc/mochi-thallium) C++ wrappers to Mercury, Margo, and Argobots
* [ssg](https://github.com/mochi-hpc/mochi-ssg) group management for server identification
* [abt-io](https://github.com/mochi-hpc/mochi-abt-io) providing I/O bindings for Argobots
* [bedrock](https://github.com/mochi-hpc/mochi-bedrock) the standard Mochi bootstrapping service
* For the provider, MPI to launch the service.

We find the [spack](https://spack.readthedocs.io/en/latest/) package manager
very helpful for building everything you need.  We have some additional
packages in the [Mochi-repo](https://github.com/mochi-hpc/mochi-spack-packages)
Once
you have both spack and our extra repo installed you can build all the benvolio
prerequisites with

    spack install mochi-bedrock ^mochi-ssg+mpi~pmix
    spack load -r mochi-bedrock


### building

With all the dependencies in place, the rest is straightforward:

* generate the configure script with `autoreconf -fi`
* mkdir build/
* `../configure  --prefix=...`
* `make all install tests`


### Simple execution

There are two parts to benvolio: the _provider_ (sometimes called the _server_)
and the _client_ .  A more complicated example would run the provider on one or
more nodes and the client on one or more (possibly the same) node.  In this
case we are just going to run everything on one node.

#### Provider with bedrock

Benvolio and its dependencies have a lot of tuning parameters.  Mochi services
use `bedrock` to capture both those dependencies and their configuration in a
JSON file.  Benvolio provides a
[starter JSON file](tests/bv-bedrock-server.json).  If necessary, add
Benvolio's bedrock modules to your `LD_LIBRARY_PATH`.  Then you can start up a
bedrock-enabled provider like this:

    bedrock  -c ../tests/bv-bedrock-server.json tcp

The provider(s) will leave an SSG group file ( bv-svc.ssg by default, but
configurable) which the clients (see below) can use to find providers.

#### Client

1. A simple benvolio test:  `tests/simple` runs through a few I/O patterns.  It
has one mandatory argument: the name of the provider statefile.  You can give
it an optional file name, or it will write to and read from a file called
'dummy' by default. If you see `Error: PMIx event notification registration
failed! [-31]` you can ignore that for now.  We're still investigating
( <https://xgitlab.cels.anl.gov/sds/benvolio/issues/2> )

   $ ./tests/simple bv-svc.ssg


2. cleanup.  Bedrock takes care of both startup and shutdown:

    $ bedrock-shutdown bv.svc tcp


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
Instrumentation](https://github.com/mochi-hpc/mochi-margo/blob/main/doc/instrumentation.md)
