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
