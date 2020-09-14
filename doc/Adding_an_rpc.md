# Adding a new RPC to Benvolio

Let's walk through the process of adding an RPC to benvolio

## Background

If benvolio were using mercury or margo directly, a new rpc would require a bit
of work.  We would have to register RPCs and define serialization and
deserialization structures.  Benvolio, though, uses thallium to handle a lot of
the grunt work.


## Implementing 'ping'

I wanted a quick way to tell if the benvolio providers were up and healthy.  I
could try to read from a nonexistent file, but a real ping rpc will let us even
more.  In addition to a binary "up/down" result we can also report how many
processes are in the provider's group.


You can find more information about thallium and "provider objects" in the
thallium
[documentation](https://mochi.readthedocs.io/en/latest/thallium/09_providers.html)

### Server side

- add a `ping` method to the `bv_svc_provider` struct.
  In this simple RPC we can have no
  arguments and only a single return value.   RPCs that operate on files should
  take a file name argument `(const std::string &file)` and either operate on
  the file name directly, or check to see if benvolio's file descriptor cache
  already has it  with the `getfd` routine ).
- in the constructor, call 'define' to register the new method with thallium
- add the thallium `remote_procedure` to the `rpcs` list: benvolio will use that
  list to deregister all the RPCs at exit.
- Note: benvolio combines the `define` and `push_back` calls so you can do the above two steps in one line e.g:
    rpcs.push_back(define("myrpc", &bv_svc_provider::my_rpc_function)

### Client side

Unlike the providers, clients need a way to pick out individual rpcs.
- Add a `tl::remote_procedure` member to the `bv_client` struct.
- Initialize that new member in the `bv_client` constructor
- `deregister` the new RPC in the `bv_client` destructor

That's all it takes to set up the RPC itself, but client code will need a way to invoke it.
- provide an implementation of `bv_ping` in `bv-client.cc`
- all benvolio client apis take a `bv_client_t` argument.  This client context
  came from `bv_init`
- this implementation invokes the RPC by using the `on` method.  We might have
  several targets we wish to `ping`: we can iterate through all the providers
  like this:

    ```
        int ret = 0;
        for (auto target : client->targets)
	    ret += client->ping_op.on(target)().as<int>();

        return ret;
    ```

- add a prototype in bv.h

Finally, you will need a way to exercise this new routine.  You can add it to
the `tests/simple.c` test or write a new utility.  The whole point of adding
this ping RPC was to be able to use a utility program, so we will write one and
add it to the build system.  One can refer to `src/client/bv-shutdown.c`
for a good example of such a utility.
