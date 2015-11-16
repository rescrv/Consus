// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_txman_h_
#define consus_common_txman_h_

// po6
#include <po6/net/location.h>

// consus
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

class txman
{
    public:
        txman();
        txman(comm_id id, const po6::net::location& bind_to);
        txman(const txman& other);
        ~txman() throw ();

    public:
        comm_id id;
        po6::net::location bind_to;
        data_center_id dc;
};

std::ostream&
operator << (std::ostream& lhs, const txman& rhs);

e::packer
operator << (e::packer lhs, const txman& rhs);
e::unpacker
operator >> (e::unpacker lhs, txman& rhs);
size_t
pack_size(const txman& p);

END_CONSUS_NAMESPACE

#endif // consus_common_txman_h_
