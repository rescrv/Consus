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

// C
#include <stdlib.h>

// e
#include <e/base64.h>
#include <e/endian.h>
#include <e/varint.h>

// consus
#include "common/transaction_group.h"

using consus::transaction_group;

transaction_group :: transaction_group()
    : group()
    , txid()
{
}

transaction_group :: transaction_group(transaction_id t)
    : group(t.group)
    , txid(t)
{
}

transaction_group :: transaction_group(paxos_group_id g, transaction_id t)
    : group(g)
    , txid(t)
{
}

transaction_group :: transaction_group(const transaction_group& other)
    : group(other.group)
    , txid(other.txid)
{
}

transaction_group :: ~transaction_group() throw ()
{
}

size_t
transaction_group :: hash() const
{
    e::compat::hash<uint64_t> h;
    return h(group.get()) ^ txid.hash();
}

int
transaction_group :: compare(const transaction_group& rhs) const
{
    if (group < rhs.group) return -1;
    if (group > rhs.group) return 1;
    if (txid < rhs.txid) return -1;
    if (txid > rhs.txid) return 1;
    return 0;
}

std::string
transaction_group :: log(const transaction_group& tg)
{
    unsigned char buf[sizeof(uint64_t) + 2 * VARINT_64_MAX_SIZE];
    unsigned char* ptr = buf;
    ptr = e::pack64be(tg.txid.number, ptr);
    ptr = e::packvarint64(tg.txid.group.get(), ptr);
    ptr = e::packvarint64(tg.group.get(), ptr);
    char b64[2 * sizeof(buf)];
    size_t sz = e::b64_ntop(buf, ptr - buf, b64, sizeof(b64));
    assert(sz <= sizeof(b64));
    return std::string(b64, sz);
}

std::ostream&
consus :: operator << (std::ostream& lhs, const transaction_group& rhs)
{
    return lhs << "transaction_group(executing="
               << rhs.group << ", originating="
               << rhs.txid.group << ", start="
               << rhs.txid.start << ", number="
               << rhs.txid.number << ")";
}

e::packer
consus :: operator << (e::packer pa, const transaction_group& rhs)
{
    return pa << rhs.group << rhs.txid;
}

e::unpacker
consus :: operator >> (e::unpacker up, transaction_group& rhs)
{
    return up >> rhs.group >> rhs.txid;
}

size_t
consus :: pack_size(const transaction_group& tg)
{
    return pack_size(tg.group) + pack_size(tg.txid);
}
