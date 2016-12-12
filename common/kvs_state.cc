// Copyright (c) 2015-2016, Robert Escriva, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Consus nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

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
