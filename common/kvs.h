// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_kvs_h_
#define consus_common_kvs_h_

// po6
#include <po6/net/location.h>

// consus
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

class kvs
{
    public:
        kvs();
        kvs(comm_id id, const po6::net::location& bind_to);
        kvs(const kvs& other);
        ~kvs() throw ();

    public:
        comm_id id;
        po6::net::location bind_to;
        data_center_id dc;
};

std::ostream&
operator << (std::ostream& lhs, const kvs& rhs);

e::packer
operator << (e::packer lhs, const kvs& rhs);
e::unpacker
operator >> (e::unpacker lhs, kvs& rhs);
size_t
pack_size(const kvs& p);

END_CONSUS_NAMESPACE

#endif // consus_common_kvs_h_
