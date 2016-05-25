// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/ids.h"

#define CREATE_ID(TYPE) \
    std::ostream& \
    operator << (std::ostream& lhs, const TYPE ## _id& rhs) \
    { \
        return lhs << #TYPE "(" << rhs.get() << ")"; \
    } \
    e::packer \
    operator << (e::packer pa, const TYPE ## _id& rhs) \
    { \
        return pa << rhs.get(); \
    } \
    e::unpacker \
    operator >> (e::unpacker up, TYPE ## _id& rhs) \
    { \
        uint64_t id; \
        up = up >> id; \
        rhs = TYPE ## _id(id); \
        return up; \
    }

BEGIN_CONSUS_NAMESPACE

CREATE_ID(abstract)
CREATE_ID(cluster)
CREATE_ID(version)
CREATE_ID(comm)
CREATE_ID(paxos_group)
CREATE_ID(data_center)
CREATE_ID(partition)

END_CONSUS_NAMESPACE
