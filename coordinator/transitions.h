/* Copyright (c) 2015-2016, Robert Escriva, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Consus nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
TRANSITION(kvs_migrated);

TRANSITION(is_stable);
TRANSITION(tick);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* consus_coordinator_transitions_h_ */
