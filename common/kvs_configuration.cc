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

// STL
#include <sstream>

// consus
#include "common/kvs_configuration.h"

e::unpacker
consus :: kvs_configuration(e::unpacker up,
                            cluster_id* cid,
                            version_id* vid,
                            uint64_t* flags,
                            std::vector<kvs_state>* kvss,
                            std::vector<ring>* rings)
{
    return up >> *cid >> *vid >> *flags >> *kvss >> *rings;
}

std::string
consus :: kvs_configuration(const cluster_id& cid,
                              const version_id& vid,
                              uint64_t,
                              const std::vector<kvs_state>& kvss,
                              const std::vector<ring>& rings)
{
    std::ostringstream ostr;
    ostr << cid << "\n"
         << vid << "\n";

    if (kvss.empty())
    {
        ostr << "no key-value stores\n";
    }
    else if (kvss.size() == 1)
    {
        ostr << "1 key-value store:\n";
    }
    else
    {
        ostr << kvss.size() << " key-value stores:\n";
    }

    for (size_t i = 0; i < kvss.size(); ++i)
    {
        ostr << kvss[i] << "\n";
    }

    for (size_t i = 0; i < rings.size(); ++i)
    {
        ostr << "ring for " << rings[i].dc << "\n";
        const partition* ptr = rings[i].partitions;
        const partition* const end = ptr + CONSUS_KVS_PARTITIONS;

        while (ptr < end)
        {
            const partition* eor = ptr;

            while (eor < end &&
                   ptr->owner == eor->owner &&
                   ptr->next_owner == eor->next_owner)
            {
                ++eor;
            }

            unsigned ub = eor < end ? eor->index : CONSUS_KVS_PARTITIONS;

            if (ptr + 1 == eor && ptr->next_owner == comm_id())
            {
                ostr << "partition[" << ptr->index << "] mannaged by " << ptr->owner;
            }
            else if (ptr + 1 == eor)
            {
                ostr << "partition[" << ptr->index << "] migrating from " << ptr->owner << " to " << ptr->next_owner;
            }
            else if (ptr->next_owner == comm_id())
            {
                ostr << "partition[" << ptr->index << ":" << ub << "] mannaged by " << ptr->owner;
            }
            else
            {
                ostr << "partition[" << ptr->index << ":" << ub << "] migrating from " << ptr->owner << " to " << ptr->next_owner;
            }

            ostr << "\n";
            ptr = eor;
        }
    }

    return ostr.str();
}
