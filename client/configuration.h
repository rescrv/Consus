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

#ifndef consus_client_configuration_h_
#define consus_client_configuration_h_

// C
#include <stdint.h>

// po6
#include <po6/net/location.h>

// e
#include <e/buffer.h>

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/txman.h"
#include "client/server_selector.h"

BEGIN_CONSUS_NAMESPACE

class configuration
{
    public:
        configuration();
        configuration(const configuration& other);
        ~configuration() throw ();

    // metadata
    public:
        cluster_id cluster() const { return m_cluster; }
        version_id version() const { return m_version; }

    // transaction managers
    public:
        bool exists(const comm_id& id) const;
        po6::net::location get_address(const comm_id& id) const;
        void initialize(server_selector* ss);

    public:
        configuration& operator = (const configuration& rhs);

    private:
        friend e::unpacker operator >> (e::unpacker, configuration& s);

    private:
        cluster_id m_cluster;
        version_id m_version;
        uint64_t m_flags;
        std::vector<txman> m_txmans;
};

std::ostream&
operator << (std::ostream& lhs, const configuration& rhs);
e::unpacker
operator >> (e::unpacker lhs, configuration& rhs);

END_CONSUS_NAMESPACE

#endif // consus_client_configuration_h_
