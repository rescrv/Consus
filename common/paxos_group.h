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

#ifndef consus_common_paxos_group_h_
#define consus_common_paxos_group_h_

// consus
#include "namespace.h"
#include "common/constants.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

class paxos_group
{
    public:
        paxos_group();
        paxos_group(const paxos_group& other);
        ~paxos_group() throw ();

    public:
        unsigned quorum() const;
        unsigned index(comm_id id) const;

    public:
        paxos_group& operator = (const paxos_group& rhs);

    public:
        paxos_group_id id;
        data_center_id dc;
        unsigned members_sz;
        comm_id members[CONSUS_MAX_REPLICATION_FACTOR];
};

std::ostream&
operator << (std::ostream& lhs, const paxos_group& rhs);

e::packer
operator << (e::packer lhs, const paxos_group& rhs);
e::unpacker
operator >> (e::unpacker lhs, paxos_group& rhs);
size_t
pack_size(const paxos_group& p);

END_CONSUS_NAMESPACE

#endif // consus_common_paxos_group_h_
