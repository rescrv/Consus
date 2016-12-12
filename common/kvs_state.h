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
            OFFLINE     = 3,
            RETIRING    = 4
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
