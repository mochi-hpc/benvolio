#ifndef COMMON_H
#define COMMON_H

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

#ifndef MAX
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#endif

#ifndef HG_INIT_INFO_INITIALIZER
/* backwards portability: 'struct na_init_info as defined in
 * mercury-2.0.0a1: subsequent versions define and provide an
 * INITIALIZER for these structs.  */
#define NA_INIT_INFO_INITIALIZER { NA_DEFAULT, 1, NULL}
#define HG_INIT_INFO_INITIALIZER { NA_INIT_INFO_INITIALIZER, NULL, HG_FALSE, HG_FALSE }
#endif


#endif
