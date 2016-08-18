// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "common/consus.h"
#include "common/macros.h"

std::ostream&
consus :: operator << (std::ostream& lhs, const consus_returncode& rc)
{
    switch (rc)
    {
        STRINGIFY(CONSUS_SUCCESS);
        STRINGIFY(CONSUS_LESS_DURABLE);
        STRINGIFY(CONSUS_NOT_FOUND);
        STRINGIFY(CONSUS_ABORTED);
        STRINGIFY(CONSUS_COMMITTED);
        STRINGIFY(CONSUS_UNKNOWN_TABLE);
        STRINGIFY(CONSUS_NONE_PENDING);
        STRINGIFY(CONSUS_INVALID);
        STRINGIFY(CONSUS_TIMEOUT);
        STRINGIFY(CONSUS_INTERRUPTED);
        STRINGIFY(CONSUS_SEE_ERRNO);
        STRINGIFY(CONSUS_COORD_FAIL);
        STRINGIFY(CONSUS_UNAVAILABLE);
        STRINGIFY(CONSUS_SERVER_ERROR);
        STRINGIFY(CONSUS_INTERNAL);
        STRINGIFY(CONSUS_GARBAGE);
        default:
            lhs << "unknown consus_returncode";
    }

    return lhs;
}

e::packer
consus :: operator << (e::packer lhs, const consus_returncode& rhs)
{
    uint16_t mt = static_cast<uint16_t>(rhs);
    return lhs << mt;
}

e::unpacker
consus :: operator >> (e::unpacker lhs, consus_returncode& rhs)
{
    uint16_t mt;
    lhs = lhs >> mt;
    rhs = static_cast<consus_returncode>(mt);
    return lhs;
}

size_t
consus :: pack_size(const consus_returncode&)
{
    return sizeof(uint16_t);
}
