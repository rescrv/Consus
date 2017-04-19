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

#ifndef consus_kvs_replica_set_h_
#define consus_kvs_replica_set_h_

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/kvs_state.h"
#include "common/ring.h"

BEGIN_CONSUS_NAMESPACE

class replica_set
{
    public:
        replica_set();
        ~replica_set() throw ();

    public:
        unsigned index(comm_id id) const;

    public:
        unsigned num_replicas;
        unsigned desired_replication;
        comm_id replicas[CONSUS_MAX_REPLICATION_FACTOR];
        comm_id transitioning[CONSUS_MAX_REPLICATION_FACTOR];
};

// Does the "target" in both a and b have the same "owner"/"next_owner" setup in
// both replica sets.  This guarantees that the host driving the replication and
// the host that was the recipient of a raw read/write agree on the portion of
// the quorum pertaining to that host.
bool
replica_sets_agree(comm_id target,
                   const replica_set& a,
                   const replica_set& b);

bool
operator == (const replica_set& lhs, const replica_set& rhs);
inline bool
operator != (const replica_set& lhs, const replica_set& rhs)
{ return !(lhs == rhs); }

std::ostream&
operator << (std::ostream& lhs, const replica_set& rhs);

e::packer
operator << (e::packer lhs, const replica_set& rhs);
e::unpacker
operator >> (e::unpacker lhs, replica_set& rhs);
size_t
pack_size(const replica_set& r);

END_CONSUS_NAMESPACE

#endif // consus_kvs_replica_set_h_
