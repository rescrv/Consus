// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_client_configuration_h_
#define consus_common_client_configuration_h_

// consus
#include "namespace.h"
#include "common/ids.h"
#include "common/txman.h"

BEGIN_CONSUS_NAMESPACE

e::unpacker client_configuration(e::unpacker up,
                                 cluster_id* cid,
                                 version_id* vid,
                                 uint64_t* flags,
                                 std::vector<txman>* txmans);

END_CONSUS_NAMESPACE

#endif // consus_common_client_configuration_h_
