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

#ifndef consus_kvs_datalayer_h_
#define consus_kvs_datalayer_h_

// e
#include <e/slice.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "common/lock.h"
#include "common/transaction_group.h"

BEGIN_CONSUS_NAMESPACE

class datalayer
{
    public:
        class reference;

    public:
        datalayer();
        virtual ~datalayer() throw ();

    public:
        virtual bool init(std::string data) = 0;
        virtual consus_returncode get(const e::slice& table,
                                      const e::slice& key,
                                      uint64_t timestamp_le,
                                      uint64_t* timestamp,
                                      e::slice* value,
                                      reference** ref) = 0;
        virtual consus_returncode put(const e::slice& table,
                                      const e::slice& key,
                                      uint64_t timestamp,
                                      const e::slice& value) = 0;
        virtual consus_returncode del(const e::slice& table,
                                      const e::slice& key,
                                      uint64_t timestamp) = 0;
        virtual consus_returncode read_lock(const e::slice& table,
                                            const e::slice& key,
                                            transaction_group* tg) = 0;
        virtual consus_returncode write_lock(const e::slice& table,
                                             const e::slice& key,
                                             const transaction_group& tg) = 0;
};

class datalayer::reference
{
    public:
        reference();
        virtual ~reference() throw ();
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_datalayer_h_
