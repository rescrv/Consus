// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/macros.h"
#include "common/kvs_state.h"

using consus::kvs_state;

const char*
kvs_state :: to_string(state_t state)
{
    switch (state)
    {
        CSTRINGIFY(REGISTERED);
        CSTRINGIFY(ONLINE);
        CSTRINGIFY(OFFLINE);
        CSTRINGIFY(RETIRING);
        default:
            return "UNKNOWN";
    }
}

kvs_state :: kvs_state()
    : kv()
    , state()
    , nonce()
{
}

kvs_state :: kvs_state(const kvs& k)
    : kv(k)
    , state(REGISTERED)
    , nonce()
{
}

kvs_state :: kvs_state(const kvs_state& other)
    : kv(other.kv)
    , state(other.state)
    , nonce(other.nonce)
{
}

kvs_state :: ~kvs_state() throw ()
{
}

kvs_state&
kvs_state :: operator = (const kvs_state& rhs)
{
    if (this != &rhs)
    {
        kv = rhs.kv;
        state = rhs.state;
        nonce = rhs.nonce;
    }

    return *this;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const kvs_state& rhs)
{
    return lhs << "kvs(id=" << rhs.kv.id.get()
               << ", bind_to=" << rhs.kv.bind_to
               << ", state=" << rhs.state
               << ", nonce=" << rhs.nonce
               << ")";
}

std::ostream&
consus :: operator << (std::ostream& lhs, const kvs_state::state_t& rhs)
{
    switch (rhs)
    {
        STRINGIFYNS(kvs_state, REGISTERED);
        STRINGIFYNS(kvs_state, ONLINE);
        STRINGIFYNS(kvs_state, OFFLINE);
        STRINGIFYNS(kvs_state, RETIRING);
        default:
            lhs << "unknown";
    }

    return lhs;
}

e::packer
consus :: operator << (e::packer lhs, const kvs_state& rhs)
{
    return lhs << rhs.kv << rhs.state << rhs.nonce;
}

e::unpacker
consus :: operator >> (e::unpacker lhs, kvs_state& rhs)
{
    return lhs >> rhs.kv >> rhs.state >> rhs.nonce;
}

e::packer
consus :: operator << (e::packer lhs, const kvs_state::state_t& rhs)
{
    return lhs << e::pack_uint8<kvs_state::state_t>(rhs);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, kvs_state::state_t& rhs)
{
    return lhs >> e::unpack_uint8<kvs_state::state_t>(rhs);
}
