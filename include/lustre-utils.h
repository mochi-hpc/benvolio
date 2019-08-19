#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

int lustre_getstripe(const char * filename, int32_t *stripe_size, int32_t *stripe_count);

#ifdef __cplusplus
}
#endif
