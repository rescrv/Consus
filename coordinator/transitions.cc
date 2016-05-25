// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// C++
#include <new>

// e
#include <e/guard.h>

// consus
#include "visibility.h"
#include "common/txman.h"
#include "coordinator/coordinator.h"
#include "coordinator/transitions.h"
#include "coordinator/util.h"

using namespace consus;

#define PROTECT_UNINITIALIZED \
    coordinator* c = static_cast<coordinator*>(obj); \
    e::guard g_c = e::makeobjguard(*c, &coordinator::invariant_check, ctx); \
    do \
    { \
        c->invariant_check(ctx); \
        if (c->cluster() == cluster_id()) \
        { \
            rsm_log(ctx, "cluster not initialized\n"); \
            return generate_response(ctx, consus::COORD_UNINITIALIZED); \
        } \
    } \
    while (0)

#define CHECK_UNPACK(MSGTYPE) \
    do \
    { \
        if (up.error() || up.remain()) \
        { \
            rsm_log(ctx, "received malformed \"" #MSGTYPE "\" message\n"); \
            return generate_response(ctx, consus::COORD_MALFORMED); \
        } \
    } while (0)

extern "C"
{

CONSUS_API void*
consus_coordinator_create(rsm_context* ctx)
{
    rsm_cond_create(ctx, "clientconf");
    rsm_cond_create(ctx, "txmanconf");
    rsm_cond_create(ctx, "kvsconf");
    return new (std::nothrow) coordinator();
}

CONSUS_API void*
consus_coordinator_recreate(rsm_context* ctx,
                            const char* data, size_t data_sz)
{
    return coordinator::recreate(ctx, data, data_sz);
}

CONSUS_API int
consus_coordinator_snapshot(rsm_context* ctx,
                            void* obj, char** data, size_t* data_sz)
{
    coordinator* c = static_cast<coordinator*>(obj);
    return c->snapshot(ctx, data, data_sz);
}

CONSUS_API void
consus_coordinator_init(rsm_context* ctx,
                        void* obj, const char* data, size_t data_sz)
{
    coordinator* c = static_cast<coordinator*>(obj);
    std::string id(data, data_sz);
    uint64_t cluster = strtoull(id.c_str(), NULL, 0);
    c->init(ctx, cluster);
}

CONSUS_API void
consus_coordinator_data_center_create(rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    e::slice name;
    e::unpacker up(data, data_sz);
    up = up >> name;
    CHECK_UNPACK(data_center_create);
    c->data_center_create(ctx, name.str());
}

CONSUS_API void
consus_coordinator_data_center_default(rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    e::slice name;
    e::unpacker up(data, data_sz);
    up = up >> name;
    CHECK_UNPACK(data_center_default);
    c->data_center_default(ctx, name.str());
}

CONSUS_API void
consus_coordinator_txman_register(rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    comm_id id;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> id >> bind_to;
    CHECK_UNPACK(txman_register);
    txman t(id, bind_to);
    c->txman_register(ctx, t);
}

CONSUS_API void
consus_coordinator_txman_online(rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    comm_id id;
    po6::net::location bind_to;
    uint64_t nonce;
    e::unpacker up(data, data_sz);
    up = up >> id >> bind_to >> nonce;
    CHECK_UNPACK(txman_online);
    c->txman_online(ctx, id, bind_to, nonce);
}

CONSUS_API void
consus_coordinator_txman_offline(rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    comm_id id;
    po6::net::location bind_to;
    uint64_t nonce;
    e::unpacker up(data, data_sz);
    up = up >> id >> bind_to >> nonce;
    CHECK_UNPACK(txman_offline);
    c->txman_offline(ctx, id, bind_to, nonce);
}

CONSUS_API void
consus_coordinator_kvs_register(rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    comm_id id;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> id >> bind_to;
    CHECK_UNPACK(kvs_register);
    kvs k(id, bind_to);
    c->kvs_register(ctx, k);
}

CONSUS_API void
consus_coordinator_kvs_online(rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    comm_id id;
    po6::net::location bind_to;
    uint64_t nonce;
    e::unpacker up(data, data_sz);
    up = up >> id >> bind_to >> nonce;
    CHECK_UNPACK(kvs_online);
    c->kvs_online(ctx, id, bind_to, nonce);
}

CONSUS_API void
consus_coordinator_kvs_offline(rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    comm_id id;
    po6::net::location bind_to;
    uint64_t nonce;
    e::unpacker up(data, data_sz);
    up = up >> id >> bind_to >> nonce;
    CHECK_UNPACK(kvs_offline);
    c->kvs_offline(ctx, id, bind_to, nonce);
}

CONSUS_API void
consus_coordinator_kvs_migrated(rsm_context* ctx, void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    partition_id id;
    e::unpacker up(data, data_sz);
    up = up >> id;
    CHECK_UNPACK(kvs_migrated);
    c->kvs_migrated(ctx, id);
}

CONSUS_API void
consus_coordinator_tick(rsm_context* ctx, void* obj, const char*, size_t)
{
    PROTECT_UNINITIALIZED;
    c->tick(ctx);
}

} // extern "C"
