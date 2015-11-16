/* Copyright (c) 2015, Robert Escriva
 * All rights reserved.
 */

#ifndef consus_coordinator_transitions_h_
#define consus_coordinator_transitions_h_

/* Replicant */
#include <rsm.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

void*
consus_coordinator_create(struct rsm_context* ctx);

void*
consus_coordinator_recreate(struct rsm_context* ctx,
                            const char* data, size_t data_sz);

int
consus_coordinator_snapshot(struct rsm_context* ctx,
                            void* obj, char** data, size_t* sz);

#define TRANSITION(X) void \
    consus_coordinator_ ## X(struct rsm_context* ctx, \
                             void* obj, const char* data, size_t data_sz)

TRANSITION(init);

TRANSITION(data_center_create);
TRANSITION(data_center_default);

TRANSITION(txman_register);
TRANSITION(txman_online);
TRANSITION(txman_offline);

TRANSITION(kvs_register);
TRANSITION(kvs_online);
TRANSITION(kvs_offline);

TRANSITION(tick);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* consus_coordinator_transitions_h_ */
