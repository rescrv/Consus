// Copyright (c) 2015-2016, Robert Escriva, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Consus nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

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
