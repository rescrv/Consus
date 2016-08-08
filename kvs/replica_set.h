// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_replica_set_h_
#define consus_kvs_replica_set_h_

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/kvs_state.h"
#include "common/ring.h"

BEGIN_CONSUS_NAMESPACE

class replica_set
{
    public:
        replica_set();
        ~replica_set() throw ();

    public:
        unsigned index(comm_id id) const;

    public:
        unsigned num_replicas;
        unsigned desired_replication;
        comm_id replicas[CONSUS_MAX_REPLICATION_FACTOR];
        comm_id transitioning[CONSUS_MAX_REPLICATION_FACTOR];
};

// Does the "target" in both a and b have the same "owner"/"next_owner" setup in
// both replica sets.  This guarantees that the host driving the replication and
// the host that was the recipient of a raw read/write agree on the portion of
// the quorum pertaining to that host.
bool
replica_sets_agree(comm_id target,
                   const replica_set& a,
                   const replica_set& b);

e::packer
operator << (e::packer lhs, const replica_set& rhs);
e::unpacker
operator >> (e::unpacker lhs, replica_set& rhs);
size_t
pack_size(const replica_set& r);

END_CONSUS_NAMESPACE

#endif // consus_kvs_replica_set_h_
