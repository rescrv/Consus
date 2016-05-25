// Copyright (c) 2016, Robert Escriva
// All rights reserved.

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
