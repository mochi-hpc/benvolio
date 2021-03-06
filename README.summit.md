# Workarounds for Summit

We currently know about four issues with the Mochi stack on the ORNL Summit machine

*MPICH and JSON* The MPICH library uses json to process a configuration script.
Towards the end of 2020, the Mochi project also started using JSON.
MPICH-3.4.1 and newer have a fix, but MPICH-3.4.1 and UCX are currently broken on Summit.  Use RobL's benvolio snapshot, or 
use `mochi@0.7.2`, which does not have any json
configuration strings.

*Libfabric*   We have a few workarounds
 * `export FI_MR_CACHE_MAX_COUNT=0`:  At one point the synchronization for the memory registration cache was outweighing the benefits.  libfabric has seen a lot of memory registration cache changes in recent releases, but we haven't measured if they help or not.
 * `export FI_OFI_RXM_USE_SRX=1`:  should help with scalability
 * `FI_VERBS_DEVICE_NAME=mlx5_0`: summit nodes have two physical infiniband cards each with a virtual port (total of four).  libfabric 1.11.1 gets confused and picks the wrong one by default.   You can also specify this on the mercury address line.


*darshan*  The default darshan is compiled against spectrum MPI.  If you are
going to build your own MPICH with the benvolio driver, you will end up with
errors about `Invalid Communicator` when the darshan MPI routines try to call
into MPICH's library and vice versa.  Darshan is great: build it yourself from
spack with a dependency on your own MPICH installation

*mercury*  Checksumming in Mercury is slow on non-x86 platforms.  Build
mercury with the 'checksum' variant disabled

*argobots*  `argobot@1.0` used a "weakly atomic" mutex on powerpc .  That means a mutex might yield even if there is no contention.  The result was that some margo operations (`margo_forward_timed`) would occasionally take a really long time to complete.  `argobots@1.1b1` has fixed this issue.
