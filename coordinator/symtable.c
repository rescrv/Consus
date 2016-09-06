/* Copyright (c) 2015, Robert Escriva
 * All rights reserved.
 */

/* Replicant */
#include <rsm.h>

/* consus */
#include "visibility.h"
#include "coordinator/transitions.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"

struct state_machine CONSUS_API rsm = {
    consus_coordinator_create,
    consus_coordinator_recreate,
    consus_coordinator_snapshot,
    {{"init", consus_coordinator_init},
     {"data_center_create", consus_coordinator_data_center_create},
     {"data_center_default", consus_coordinator_data_center_default},
     {"txman_register", consus_coordinator_txman_register},
     {"txman_online", consus_coordinator_txman_online},
     {"txman_offline", consus_coordinator_txman_offline},
     {"kvs_register", consus_coordinator_kvs_register},
     {"kvs_online", consus_coordinator_kvs_online},
     {"kvs_offline", consus_coordinator_kvs_offline},
     {"kvs_migrated", consus_coordinator_kvs_migrated},
     {"is_stable", consus_coordinator_is_stable},
     {"tick", consus_coordinator_tick},
     {NULL, NULL}}
};

#pragma GCC diagnostic pop
