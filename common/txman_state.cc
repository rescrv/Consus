// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/macros.h"
#include "common/txman_state.h"

using consus::txman_state;

const char*
txman_state :: to_string(state_t state)
{
    switch (state)
    {
        CSTRINGIFY(REGISTERED);
        CSTRINGIFY(ONLINE);
        CSTRINGIFY(OFFLINE);
        default:
            return "UNKNOWN";
    }
}

txman_state :: txman_state()
    : tx()
    , state()
    , nonce()
{
}

txman_state :: txman_state(const txman& t)
    : tx(t)
    , state(REGISTERED)
    , nonce()
{
}

txman_state :: txman_state(const txman_state& other)
    : tx(other.tx)
    , state(other.state)
    , nonce(other.nonce)
{
}

txman_state :: ~txman_state() throw ()
{
}

txman_state&
txman_state :: operator = (const txman_state& rhs)
{
    if (this != &rhs)
    {
        tx = rhs.tx;
        state = rhs.state;
        nonce = rhs.nonce;
    }

    return *this;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const txman_state& rhs)
{
    return lhs << "txman(id=" << rhs.tx.id.get()
               << ", bind_to=" << rhs.tx.bind_to
               << ", dc=" << rhs.tx.dc.get()
               << ", state=" << rhs.state
               << ", nonce=" << rhs.nonce
               << ")";
}

std::ostream&
consus :: operator << (std::ostream& lhs, const txman_state::state_t& rhs)
{
    switch (rhs)
    {
        STRINGIFYNS(txman_state, REGISTERED);
        STRINGIFYNS(txman_state, ONLINE);
        STRINGIFYNS(txman_state, OFFLINE);
        default:
            lhs << "unknown";
    }

    return lhs;
}

e::packer
consus :: operator << (e::packer lhs, const txman_state& rhs)
{
    return lhs << rhs.tx << rhs.state << rhs.nonce;
}

e::unpacker
consus :: operator >> (e::unpacker lhs, txman_state& rhs)
{
    return lhs >> rhs.tx >> rhs.state >> rhs.nonce;
}

e::packer
consus :: operator << (e::packer lhs, const txman_state::state_t& rhs)
{
    return lhs << e::pack_uint8<txman_state::state_t>(rhs);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, txman_state::state_t& rhs)
{
    return lhs >> e::unpack_uint8<txman_state::state_t>(rhs);
}
