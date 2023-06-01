#include <ssg.h>
#include <abt-io.h>
#include "bv-provider.h"
#include <bedrock/module.h>

/* declared as "name", "type" */
static struct bedrock_dependency bv_provider_dependencies[] = {
    {"group", "ssg", BEDROCK_REQUIRED},
    {"storage", "abt_io", BEDROCK_REQUIRED},
    BEDROCK_NO_MORE_DEPENDENCIES
};

static struct bedrock_dependency bv_client_dependencies[] = {
    BEDROCK_NO_MORE_DEPENDENCIES
};

static int benvolio_register_provider (
        bedrock_args_t args,
        bedrock_module_provider_t* provider)
{
    margo_instance_id mid = bedrock_args_get_margo_instance(args);
    uint16_t provider_id  = bedrock_args_get_provider_id(args);
    ABT_pool pool         = bedrock_args_get_pool(args);
    const char* config    = bedrock_args_get_config(args);
    abt_io_instance_id abt_io = ABT_IO_INSTANCE_NULL;
    ssg_group_id_t gid    = SSG_GROUP_ID_INVALID;

    // bedrock checks existance for REQUIRED dependencies
    abt_io = (abt_io_instance_id)bedrock_args_get_dependency(args, "storage", 0);
    gid = (ssg_group_id_t)bedrock_args_get_dependency(args, "group", 0);

    bv_svc_provider_t tmp = BV_PROVIDER_NULL;
    int ret = bv_svc_provider_register_ext(mid, provider_id, abt_io, pool, gid, config, &tmp);
    *provider = (bv_svc_provider_t)tmp;

    return ret;
}

static int benvolio_deregister_provider(
        bedrock_module_provider_t provider)
{
    return bv_provider_destroy((bv_svc_provider_t)provider);
}

static char* benvolio_get_provider_config(
        bedrock_module_provider_t provider) {
    return bv_svc_provider_get_config(provider);
}

static int benvolio_init_client(
        bedrock_args_t args,
        bedrock_module_client_t* client)
{
    printf("Unexpected client init from bedrock\n");
    return BEDROCK_SUCCESS;
}

static int benvolio_finalize_client(
        )
{
    printf("Unexpected client finalize from bedrock\n");
    return BEDROCK_SUCCESS;
}

static char * benvolio_get_client_config(
        bedrock_module_provider_t provider) {
    printf("Unexpected client config request\n");
    return strdup("{}");
}

static int benvolio_create_provider_handle(
        bedrock_module_client_t client,
        hg_addr_t address,
        uint16_t provider_id,
        bedrock_module_provider_handle_t *ph)
{
    printf("Unexpected client provider handle create\n");
    return 0;
}

static int benvolio_destroy_provider_handle(
        bedrock_module_provider_handle_t ph)
{
    printf("Unexpected client provider handle destroy\n");
    return 0;
}


static struct bedrock_module benvolio_module = {
    .register_provider = benvolio_register_provider,
    .deregister_provider = benvolio_deregister_provider,
    .get_provider_config = benvolio_get_provider_config,
    .init_client = benvolio_init_client,
    .finalize_client = benvolio_finalize_client,
    .get_client_config = benvolio_get_client_config,
    .create_provider_handle = benvolio_create_provider_handle,
    .destroy_provider_handle = benvolio_destroy_provider_handle,
    .provider_dependencies = bv_provider_dependencies,
    .client_dependencies = bv_client_dependencies
};

BEDROCK_REGISTER_MODULE(benvolio, benvolio_module);
