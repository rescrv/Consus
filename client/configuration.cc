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
#include "common/client_configuration.h"
#include "client/configuration.h"

using consus::configuration;

configuration :: configuration()
    : m_cluster()
    , m_version()
    , m_flags(0)
    , m_txmans()
{
}

configuration :: configuration(const configuration& other)
    : m_cluster(other.m_cluster)
    , m_version(other.m_version)
    , m_flags(other.m_flags)
    , m_txmans(other.m_txmans)
{
}

configuration :: ~configuration() throw ()
{
}

po6::net::location
configuration :: get_address(const comm_id& id) const
{
    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        if (m_txmans[i].id == id)
        {
            return m_txmans[i].bind_to;
        }
    }

    return po6::net::location();
}

void
configuration :: initialize(server_selector* ss)
{
    std::vector<comm_id> ids;

    for (size_t i = 0; i < m_txmans.size(); ++i)
    {
        ids.push_back(m_txmans[i].id);
    }

    ss->set(&ids[0], ids.size());
}

configuration&
configuration :: operator = (const configuration& rhs)
{
    if (this != &rhs)
    {
        m_cluster = rhs.m_cluster;
        m_version = rhs.m_version;
        m_flags = rhs.m_flags;
        m_txmans = rhs.m_txmans;
    }

    return *this;
}

e::unpacker
consus :: operator >> (e::unpacker up, configuration& rhs)
{
    return client_configuration(up, &rhs.m_cluster, &rhs.m_version, &rhs.m_flags, &rhs.m_txmans);
}
