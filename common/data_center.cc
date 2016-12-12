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

// e
#include <e/strescape.h>

// consus
#include "common/data_center.h"

using consus::data_center;

data_center :: data_center()
    : id()
    , name()
{
}

data_center :: data_center(data_center_id i, const std::string& n)
    : id(i)
    , name(n)
{
}

data_center :: data_center(const data_center& other)
    : id(other.id)
    , name(other.name)
{
}

data_center :: ~data_center() throw ()
{
}

std::ostream&
consus :: operator << (std::ostream& lhs, const data_center& rhs)
{
    return lhs << "data_center(id=" << rhs.id.get()
               << ", name=\"" << e::strescape(rhs.name) << "\")";
}

e::packer
consus :: operator << (e::packer lhs, const data_center& rhs)
{
    return lhs << rhs.id << e::slice(rhs.name);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, data_center& rhs)
{
    e::slice name;
    lhs = lhs >> rhs.id >> name;
    rhs.name = name.str();
    return lhs;
}
