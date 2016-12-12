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

#ifndef consus_kvs_table_key_pair_h_
#define consus_kvs_table_key_pair_h_

// e
#include <e/compat.h>
#include <e/slice.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

struct table_key_pair
{
    table_key_pair();
    table_key_pair(const e::slice& table, const e::slice& key);
    ~table_key_pair() throw ();
    std::string table;
    std::string key;
};

bool
operator == (const table_key_pair& lhs, const table_key_pair& rhs);

END_CONSUS_NAMESPACE

BEGIN_E_COMPAT_NAMESPACE
template <>
struct hash<consus::table_key_pair>
{
    size_t operator()(const consus::table_key_pair& x) const
    {
        e::compat::hash<string> h;
        return h(x.table) ^ h(x.key);
    }
};
END_E_COMPAT_NAMESPACE

#endif // consus_kvs_table_key_pair_h_
