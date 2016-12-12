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
#include "common/txman.h"

using consus::txman;

txman :: txman()
    : id()
    , bind_to()
    , dc()
{
}

txman :: txman(comm_id i, const po6::net::location& b)
    : id(i)
    , bind_to(b)
    , dc()
{
}

txman :: txman(const txman& other)
    : id(other.id)
    , bind_to(other.bind_to)
    , dc(other.dc)
{
}

txman :: ~txman() throw ()
{
}

std::ostream&
consus :: operator << (std::ostream& lhs, const txman& rhs)
{
    return lhs << "txman(id=" << rhs.id.get() << ", bind_to=" << rhs.bind_to << ", dc=" << rhs.dc.get() << ")";
}

e::packer
consus :: operator << (e::packer lhs, const txman& rhs)
{
    return lhs << rhs.id << rhs.bind_to << rhs.dc;
}

e::unpacker
consus :: operator >> (e::unpacker lhs, txman& rhs)
{
    return lhs >> rhs.id >> rhs.bind_to >> rhs.dc;
}
