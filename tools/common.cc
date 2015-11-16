// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// replicant
#include <replicant.h>

// consus
#include "tools/common.h"

bool
consus :: finish(consus_client* cl, const char* prog, int64_t id, consus_returncode* status)
{
    if (id < 0)
    {
        std::cerr << prog << ": " << consus_error_message(cl) << std::endl;
        return false;
    }

    consus_returncode lrc;
    int64_t lid = consus_wait(cl, id, -1, &lrc);

    if (lid < 0)
    {
        std::cerr << prog << ": " << consus_error_message(cl) << std::endl;
        return false;
    }

    assert(id == lid);

    if (*status != CONSUS_SUCCESS)
    {
        std::cerr << prog << ": " << consus_error_message(cl) << std::endl;
        return false;
    }

    return true;
}
