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

#ifndef consus_common_ring_h_
#define consus_common_ring_h_

// consus
#include "namespace.h"
#include "common/constants.h"
#include "common/ids.h"
#include "common/partition.h"

BEGIN_CONSUS_NAMESPACE

class ring
{
    public:
        ring();
        ring(data_center_id dc);
        ~ring() throw ();

    public:
        void get_owners(comm_id owners[CONSUS_KVS_PARTITIONS]);
        void set_owners(comm_id owners[CONSUS_KVS_PARTITIONS], uint64_t* post_inc_counter);

    public:
        data_center_id dc;
        partition partitions[CONSUS_KVS_PARTITIONS];
};

e::packer
operator << (e::packer lhs, const ring& rhs);
e::unpacker
operator >> (e::unpacker lhs, ring& rhs);
size_t
pack_size(const ring& r);

END_CONSUS_NAMESPACE

#endif // consus_common_ring_h_
