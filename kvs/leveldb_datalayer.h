// Copyright (c) 2016, Robert Escriva
// All rights reserved.

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
