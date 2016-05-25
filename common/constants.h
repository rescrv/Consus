// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_constants_h_
#define consus_common_constants_h_

#define CONSUS_MAX_REPLICATION_FACTOR 9

#define CONSUS_PORT_TXMAN 22751
#define CONSUS_PORT_KVS 22761

// This defines the maximum number of key-value stores that can be within a
// single data center.  This can support a 10PB data set with just 160GB per
// partition.  Because each schema spreads across the entire ring, that's 10PB
// per schema.  If you have closer to 4TB/node, your cluster can support 256PB.
// All numbers are raw-data capacity.  Replication obviously introduces a
// multiplier on the space required for a given data set.
//
// Changing this constant will not be supported by the initial developers, as it
// only makes sense to increase it.  If you must increase it, you have a
// sufficiently large deployment that you should probably think about tasking a
// few developers with making the change, testing it, and giving any
// changes/fixes back to the project.
//
// Constant-Specific assumptions:
//  - common/partition.h
//  - assumed to fit in an unsigned int
//  - assumed to be this exact value in daemon::choose_index
#define CONSUS_KVS_PARTITIONS 65536

#define CONSUS_VOTE_ABORT 0x61626f7274000000ULL
#define CONSUS_VOTE_COMMIT 0x636f6d6d69740000ULL

#define CONSUS_WRITE_TOMBSTONE 1

#endif // consus_common_constants_h_
