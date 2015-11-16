// Copyright (c) 2015, Robert Escriva
// All rights reserved.

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
