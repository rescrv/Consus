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

#ifndef consus_kvs_leveldb_datalayer_h_
#define consus_kvs_leveldb_datalayer_h_

// STL
#include <memory>

// LevelDB
#include <leveldb/comparator.h>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>

// e
#include <e/slice.h>

// consus
#include <consus.h>
#include "namespace.h"
#include "kvs/datalayer.h"

BEGIN_CONSUS_NAMESPACE

class leveldb_datalayer : public datalayer
{
    public:
        leveldb_datalayer();
        virtual ~leveldb_datalayer() throw ();

    public:
        virtual bool init(std::string data);
        virtual consus_returncode get(const e::slice& table,
                                      const e::slice& key,
                                      uint64_t timestamp_le,
                                      uint64_t* timestamp,
                                      e::slice* value,
                                      datalayer::reference** ref);
        virtual consus_returncode put(const e::slice& table,
                                      const e::slice& key,
                                      uint64_t timestamp,
                                      const e::slice& value);
        virtual consus_returncode del(const e::slice& table,
                                      const e::slice& key,
                                      uint64_t timestamp);
        virtual consus_returncode read_lock(const e::slice& table,
                                            const e::slice& key,
                                            transaction_group* tg);
        virtual consus_returncode write_lock(const e::slice& table,
                                             const e::slice& key,
                                             const transaction_group& tg);

    private:
        struct comparator;
        struct reference;

    private:
        std::string data_key(const e::slice& table,
                             const e::slice& key,
                             uint64_t timestamp);
        std::string lock_key(const e::slice& table,
                             const e::slice& key);

    private:
        std::auto_ptr<comparator> m_cmp;
        const leveldb::FilterPolicy* m_bf;
        leveldb::DB* m_db;

    private:
        leveldb_datalayer(const leveldb_datalayer&);
        leveldb_datalayer& operator = (const leveldb_datalayer&);
};

END_CONSUS_NAMESPACE

#endif // consus_kvs_leveldb_datalayer_h_
