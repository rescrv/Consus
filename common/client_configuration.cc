// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// STL
#include <sstream>

// consus
#include "common/client_configuration.h"

e::unpacker
consus :: client_configuration(e::unpacker up,
                               cluster_id* cid,
                               version_id* vid,
                               uint64_t* flags,
                               std::vector<txman>* txmans)
{
    return up >> *cid >> *vid >> *flags >> *txmans;
}
