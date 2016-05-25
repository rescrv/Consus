/* Copyright (c) 2015, Robert Escriva
 * All rights reserved.
 */

#ifndef consus_unsafe_h_
#define consus_unsafe_h_

/* consus */
#include <consus.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

int64_t consus_unsafe_get(struct consus_client* client,
                          const char* table,
                          const char* key, size_t key_sz,
                          enum consus_returncode* status,
                          char** value, size_t* value_sz);
int64_t consus_unsafe_put(struct consus_client* client,
                          const char* table,
                          const char* key, size_t key_sz,
                          const char* value, size_t value_sz,
                          enum consus_returncode* status);
int64_t consus_unsafe_lock(struct consus_client* client,
                           const char* table,
                           const char* key, size_t key_sz,
                           enum consus_returncode* status);
int64_t consus_unsafe_unlock(struct consus_client* client,
                             const char* table,
                             const char* key, size_t key_sz,
                             enum consus_returncode* status);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* consus_unsafe_h_ */
