// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_paxos_group_h_
#define consus_common_paxos_group_h_

// consus
#include "namespace.h"
#include "common/constants.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

class paxos_group
{
    public:
        paxos_group();
        paxos_group(const paxos_group& other);
        ~paxos_group() throw ();

    public:
        unsigned quorum() const;
        unsigned index(comm_id id) const;

    public:
        paxos_group& operator = (const paxos_group& rhs);

    public:
        paxos_group_id id;
        data_center_id dc;
        unsigned members_sz;
        comm_id members[CONSUS_MAX_REPLICATION_FACTOR];
};

std::ostream&
operator << (std::ostream& lhs, const paxos_group& rhs);

e::packer
operator << (e::packer lhs, const paxos_group& rhs);
e::unpacker
operator >> (e::unpacker lhs, paxos_group& rhs);
size_t
pack_size(const paxos_group& p);

END_CONSUS_NAMESPACE

#endif // consus_common_paxos_group_h_
