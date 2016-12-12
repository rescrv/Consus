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

#ifndef consus_common_txman_state_h_
#define consus_common_txman_state_h_

// po6
#include <po6/net/location.h>

// consus
#include "namespace.h"
#include "common/txman.h"

BEGIN_CONSUS_NAMESPACE

class txman_state
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
        txman_state();
        txman_state(const txman& tx);
        txman_state(const txman_state& other);
        ~txman_state() throw ();

    public:
        comm_id id() const { return tx.id; }
        po6::net::location bind_to() const { return tx.bind_to; }

    public:
        txman_state& operator = (const txman_state& rhs);

    public:
        txman tx;
        state_t state;
        uint64_t nonce;
};

std::ostream&
operator << (std::ostream& lhs, const txman_state& rhs);
std::ostream&
operator << (std::ostream& lhs, const txman_state::state_t& rhs);

e::packer
operator << (e::packer lhs, const txman_state& rhs);
e::unpacker
operator >> (e::unpacker lhs, txman_state& rhs);
size_t
pack_size(const txman_state& p);

e::packer
operator << (e::packer lhs, const txman_state::state_t& rhs);
e::unpacker
operator >> (e::unpacker lhs, txman_state::state_t& rhs);
size_t
pack_size(const txman_state::state_t& p);

END_CONSUS_NAMESPACE

#endif // consus_common_txman_state_h_
