// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_mapper_h_
#define consus_kvs_mapper_h_

// BusyBee
#include <busybee_mapper.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE
class daemon;

class mapper : public busybee_mapper
{
    public:
        mapper(daemon* d);
        ~mapper() throw ();

    public:
        virtual bool lookup(uint64_t server_id, po6::net::location* bound_to);

    private:
        daemon* m_d;

    private:
        mapper(const mapper&);
        mapper& operator = (const mapper&);
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_mapper_h_
