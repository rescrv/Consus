// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_client_mapper_h_
#define consus_client_mapper_h_

// BusyBee
#include <busybee_mapper.h>

// consus
#include "namespace.h"
#include "client/configuration.h"

BEGIN_CONSUS_NAMESPACE

class mapper : public ::busybee_mapper
{
    public:
        mapper(const configuration* config);
        ~mapper() throw ();

    public:
        virtual bool lookup(uint64_t id, po6::net::location* addr);

    private:
        mapper(const mapper&);
        mapper& operator = (const mapper&);

    private:
        const configuration* m_config;
};

END_CONSUS_NAMESPACE

#endif // consus_client_mapper_h_
