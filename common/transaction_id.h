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

#ifndef consus_common_transaction_id_h_
#define consus_common_transaction_id_h_

// C
#include <stdint.h>

// C++
#include <iostream>

// e
#include <e/buffer.h>

// consus
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

class transaction_id
{
    public:
        transaction_id();
        transaction_id(paxos_group_id g, uint64_t start, uint64_t number);
        transaction_id(const transaction_id& other);
        ~transaction_id() throw ();

    public:
        size_t hash() const;
        bool preempts(const transaction_id& other) const;

        int compare(const transaction_id& rhs) const;
        bool operator < (const transaction_id& rhs) const { return compare(rhs) < 0; }
        bool operator <= (const transaction_id& rhs) const { return compare(rhs) <= 0; }
        bool operator == (const transaction_id& rhs) const { return compare(rhs) == 0; }
        bool operator != (const transaction_id& rhs) const { return compare(rhs) != 0; }
        bool operator >= (const transaction_id& rhs) const { return compare(rhs) >= 0; }
        bool operator > (const transaction_id& rhs) const { return compare(rhs) > 0; }

    public:
        paxos_group_id group;
        uint64_t start;
        uint64_t number;
};

std::ostream&
operator << (std::ostream& lhs, const transaction_id& rhs);

e::packer
operator << (e::packer pa, const transaction_id& rhs);
e::unpacker
operator >> (e::unpacker up, transaction_id& rhs);
size_t
pack_size(const transaction_id& txid);

END_CONSUS_NAMESPACE

BEGIN_E_COMPAT_NAMESPACE

template <>
struct hash<consus::transaction_id>
{
    size_t operator()(const consus::transaction_id& kr) const
    {
        return kr.hash();
    }
};

END_E_COMPAT_NAMESPACE

#endif // consus_common_transaction_id_h_
