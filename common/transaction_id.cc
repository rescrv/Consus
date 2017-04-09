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
#include "common/transaction_id.h"

using consus::transaction_id;

transaction_id :: transaction_id()
    : group()
    , start(0)
    , number(0)
{
}

transaction_id :: transaction_id(paxos_group_id g, uint64_t s, uint64_t n)
    : group(g)
    , start(s)
    , number(n)
{
}

transaction_id :: transaction_id(const transaction_id& other)
    : group(other.group)
    , start(other.start)
    , number(other.number)
{
}

transaction_id :: ~transaction_id() throw ()
{
}

size_t
transaction_id :: hash() const
{
    e::compat::hash<uint64_t> h;
    return h(start) ^ h(number) ^ h(group.get());
}

bool
transaction_id :: preempts(const transaction_id& other) const
{
    return start < other.start ||
           (start == other.start && number < other.number);
}

int
transaction_id :: compare(const transaction_id& rhs) const
{
    if (group < rhs.group) return -1;
    if (group > rhs.group) return 1;
    if (number < rhs.number) return -1;
    if (number > rhs.number) return 1;
    if (start < rhs.start) return -1;
    if (start > rhs.start) return 1;
    return 0;
}

std::ostream&
consus :: operator << (std::ostream& lhs, const transaction_id& rhs)
{
    return lhs << "transaction_id(group="
               << rhs.group.get() << ", start="
               << rhs.start << ", number="
               << rhs.number << ")";
}

e::packer
consus :: operator << (e::packer pa, const transaction_id& rhs)
{
    return pa << rhs.group << rhs.start << rhs.number;
}

e::unpacker
consus :: operator >> (e::unpacker up, transaction_id& rhs)
{
    return up >> rhs.group >> rhs.start >> rhs.number;
}

size_t
consus :: pack_size(const transaction_id& x)
{
    return pack_size(x.group) + 2 * sizeof(uint64_t);
}
