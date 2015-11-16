// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_kvs_state_h_
#define consus_common_kvs_state_h_

// po6
#include <po6/net/location.h>

// consus
#include "namespace.h"
#include "common/kvs.h"

BEGIN_CONSUS_NAMESPACE

class kvs_state
{
    public:
        enum state_t
        {
            REGISTERED  = 1,
            ONLINE      = 2,
            OFFLINE     = 3
        };
        static const char* to_string(state_t state);

    public:
        kvs_state();
        kvs_state(const kvs& tx);
        kvs_state(const kvs_state& other);
        ~kvs_state() throw ();

    public:
        comm_id id() const { return kv.id; }
        po6::net::location bind_to() const { return kv.bind_to; }

    public:
        kvs_state& operator = (const kvs_state& rhs);

    public:
        kvs kv;
        state_t state;
        uint64_t nonce;
};

std::ostream&
operator << (std::ostream& lhs, const kvs_state& rhs);
std::ostream&
operator << (std::ostream& lhs, const kvs_state::state_t& rhs);

e::packer
operator << (e::packer lhs, const kvs_state& rhs);
e::unpacker
operator >> (e::unpacker lhs, kvs_state& rhs);
size_t
pack_size(const kvs_state& p);

e::packer
operator << (e::packer lhs, const kvs_state::state_t& rhs);
e::unpacker
operator >> (e::unpacker lhs, kvs_state::state_t& rhs);
size_t
pack_size(const kvs_state::state_t& p);

END_CONSUS_NAMESPACE

#endif // consus_common_txman_state_h_
