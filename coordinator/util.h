// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_coordinator_util_h_
#define consus_coordinator_util_h_

// consus
#include "namespace.h"
#include "common/coordinator_returncode.h"

BEGIN_CONSUS_NAMESPACE

static inline void
generate_response(rsm_context* ctx, coordinator_returncode x)
{
    const char* ptr = NULL;

    switch (x)
    {
        case COORD_SUCCESS:
            ptr = "\x22\x80";
            break;
        case COORD_MALFORMED:
            ptr = "\x22\x81";
            break;
        case COORD_DUPLICATE:
            ptr = "\x22\x82";
            break;
        case COORD_NOT_FOUND:
            ptr = "\x22\x83";
            break;
        case COORD_UNINITIALIZED:
            ptr = "\x22\x85";
            break;
        case COORD_NO_CAN_DO:
            ptr = "\x22\x87";
            break;
        default:
            ptr = "\xff\xff";
            break;
    }

    rsm_set_output(ctx, ptr, 2);
}

#define INVARIANT_BROKEN(X) \
    fprintf(log, "invariant broken at " __FILE__ ":%d:  %s\n", __LINE__, X "\n")

END_CONSUS_NAMESPACE

#endif // consus_coordinator_util_h_
