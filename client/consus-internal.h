/* Copyright (c) 2015, Robert Escriva
 * All rights reserved.
 */

#ifndef consus_internal_h_
#define consus_internal_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

int consus_debug_client_configuration(struct consus_client* client,
                                      enum consus_returncode* status,
                                      const char** str);
int consus_debug_txman_configuration(struct consus_client* client,
                                     enum consus_returncode* status,
                                     const char** str);
int consus_debug_kvs_configuration(struct consus_client* client,
                                   enum consus_returncode* status,
                                   const char** str);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* consus_internal_h_ */
