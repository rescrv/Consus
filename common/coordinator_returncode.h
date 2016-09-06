// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_coordinator_returncode_h_
#define consus_common_coordinator_returncode_h_

// C++
#include <iostream>

// e
#include <e/serialization.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

// occupies [8832, 8960)
// these are hardcoded as byte strings in coordinator/coordinator.cc
// keep them in sync
enum coordinator_returncode
{
    COORD_SUCCESS = 8832,
    COORD_MALFORMED = 8833,
    COORD_DUPLICATE = 8834,
    COORD_NOT_FOUND = 8835,
    COORD_UNINITIALIZED = 8837,
    COORD_NO_CAN_DO = 8839
};

std::ostream&
operator << (std::ostream& lhs, coordinator_returncode rhs);

e::packer
operator << (e::packer lhs, const coordinator_returncode& rhs);
e::unpacker
operator >> (e::unpacker lhs, coordinator_returncode& rhs);
size_t
pack_size(const coordinator_returncode& rhs);

END_CONSUS_NAMESPACE

#endif // consus_common_coordinator_returncode_h_
