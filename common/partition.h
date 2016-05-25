// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_partition_h_
#define consus_common_partition_h_

// consus
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

struct partition
{
    partition();
    ~partition() throw ();

    uint16_t index;
    partition_id id;
    comm_id owner;
    partition_id next_id;
    comm_id next_owner;
};

e::packer
operator << (e::packer lhs, const partition& rhs);
e::unpacker
operator >> (e::unpacker lhs, partition& rhs);
size_t
pack_size(const partition& p);

END_CONSUS_NAMESPACE

#endif // consus_common_partition_h_
