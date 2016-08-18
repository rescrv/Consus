// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>
#include <e/serialization.h>
#include <e/strescape.h>

// consus
#include "kvs/leveldb_datalayer.h"

using consus::leveldb_datalayer;

struct leveldb_datalayer::comparator : public leveldb::Comparator
{
    comparator();
    virtual ~comparator() throw ();
    virtual int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const;
    virtual const char* Name() const { return "ConsusComparator"; }
    virtual void FindShortestSeparator(std::string*,
                                       const leveldb::Slice&) const {}
    virtual void FindShortSuccessor(std::string*) const {}
};

leveldb_datalayer :: comparator :: comparator()
{
}

leveldb_datalayer :: comparator :: ~comparator() throw ()
{
}

int
leveldb_datalayer :: comparator :: Compare(const leveldb::Slice& a, const leveldb::Slice& b) const
{
    if (a.size() < 8 || b.size() < 8)
    {
        return -1;
    }

    e::slice x(a.data(), a.size() - 8);
    e::slice y(b.data(), b.size() - 8);
    size_t sz = std::min(x.size(), y.size());
    int cmp = memcmp(x.data(), y.data(), sz);

    if (cmp < 0)
    {
        return -1;
    }
    if (cmp > 0)
    {
        return 1;
    }
    // cmp == 0; order by size now
    if (sz < y.size())
    {
        return -1;
    }
    if (sz > y.size())
    {
        return 1;
    }

    uint64_t at;
    uint64_t bt;
    e::unpack64be(a.data() + a.size() - 8, &at);
    e::unpack64be(b.data() + b.size() - 8, &bt);

    if (at > bt)
    {
        return -1;
    }
    if (at < bt)
    {
        return 1;
    }

    return 0;
}

struct leveldb_datalayer::reference : public datalayer::reference
{
    reference(std::auto_ptr<leveldb::Iterator> it);
    virtual ~reference() throw ();

    std::auto_ptr<leveldb::Iterator> it;
};

leveldb_datalayer :: reference :: reference(std::auto_ptr<leveldb::Iterator> _it)
    : datalayer::reference()
    , it(_it)
{
}

leveldb_datalayer :: reference :: ~reference() throw ()
{
}

leveldb_datalayer :: leveldb_datalayer()
    : m_cmp(new comparator())
    , m_bf(NULL)
    , m_db(NULL)
{
}

leveldb_datalayer :: ~leveldb_datalayer() throw ()
{
    if (m_bf)
    {
        delete m_bf;
    }

    if (m_db)
    {
        delete m_db;
    }
}

bool
leveldb_datalayer :: init(std::string data)
{
    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.filter_policy = m_bf = leveldb::NewBloomFilterPolicy(10);
    opts.max_open_files = std::max(sysconf(_SC_OPEN_MAX) >> 1, 1024L);
    opts.comparator = m_cmp.get();
    leveldb::Status st = leveldb::DB::Open(opts, data, &m_db);

    if (!st.ok())
    {
        LOG(ERROR) << "could not open leveldb: " << st.ToString();
        return false;
    }

    return true;
}

consus_returncode
leveldb_datalayer :: get(const e::slice& table,
                         const e::slice& key,
                         uint64_t timestamp_le,
                         uint64_t* timestamp,
                         e::slice* value,
                         datalayer::reference** ref)
{
    std::string tmp = data_key(table, key, timestamp_le);
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(leveldb::ReadOptions()));
    it->Seek(tmp);
    *timestamp = 0;
    *value = e::slice();
    *ref = NULL;

    if (!it->status().ok())
    {
        LOG(ERROR) << "leveldb error: " << it->status().ToString();
        return CONSUS_SERVER_ERROR;
    }
    else if (!it->Valid() || tmp.size() != it->key().size() ||
             memcmp(tmp.data(), it->key().data(), tmp.size() - 8) != 0)
    {
        return CONSUS_NOT_FOUND;
    }

    e::unpack64be(it->key().data() + it->key().size() - 8, timestamp);
    *value = e::slice(it->value().data(), it->value().size());
    *ref = new reference(it);

    if (value->empty())
    {
        return CONSUS_NOT_FOUND;
    }
    else
    {
        return CONSUS_SUCCESS;
    }
}

consus_returncode
leveldb_datalayer :: put(const e::slice& table,
                         const e::slice& key,
                         uint64_t timestamp,
                         const e::slice& value)
{
    assert(!value.empty()); /* XXX */
    std::string tmp = data_key(table, key, timestamp);
    leveldb::WriteOptions opts;
    opts.sync = true;
    leveldb::Status st = m_db->Put(opts, tmp, leveldb::Slice(value.cdata(), value.size()));
    consus_returncode rc;

    if (st.ok())
    {
        rc = CONSUS_SUCCESS;
    }
    else
    {
        LOG(ERROR) << "leveldb error: " << st.ToString();
        rc = CONSUS_SERVER_ERROR;
    }

    return rc;
}

consus_returncode
leveldb_datalayer :: del(const e::slice& table,
                         const e::slice& key,
                         uint64_t timestamp)
{
    std::string tmp = data_key(table, key, timestamp);
    leveldb::WriteOptions opts;
    opts.sync = true;
    leveldb::Status st = m_db->Put(opts, tmp, leveldb::Slice());
    consus_returncode rc;

    if (st.ok())
    {
        rc = CONSUS_SUCCESS;
    }
    else
    {
        LOG(ERROR) << "leveldb error: " << st.ToString();
        rc = CONSUS_SERVER_ERROR;
    }

    return rc;
}

consus_returncode
leveldb_datalayer :: read_lock(const e::slice& table,
                               const e::slice& key,
                               transaction_group* tg)
{
    std::string tmp = lock_key(table, key);
    std::string val;
    leveldb::Status st = m_db->Get(leveldb::ReadOptions(), tmp, &val);

    if (st.IsNotFound())
    {
        *tg = transaction_group();
        return CONSUS_NOT_FOUND;
    }
    else if (!st.ok())
    {
        LOG(ERROR) << "leveldb error: " << st.ToString();
        return CONSUS_SERVER_ERROR;
    }

    e::unpacker up(val);
    up = up >> *tg;

    if (up.error())
    {
        LOG(ERROR) << "corrupt lock (\""
                   << e::strescape(table.str()) << "\", \""
                   << e::strescape(key.str()) << "\")";
        return CONSUS_INVALID;
    }

    return CONSUS_SUCCESS;
}

consus_returncode
leveldb_datalayer :: write_lock(const e::slice& table,
                                const e::slice& key,
                                const transaction_group& tg)
{
    std::string tmp = lock_key(table, key);
    std::string val;
    e::packer(&val) << tg;
    leveldb::WriteOptions opts;
    opts.sync = true;
    leveldb::Status st = m_db->Put(opts, tmp, val);

    if (st.ok())
    {
        return CONSUS_SUCCESS;
    }
    else
    {
        LOG(ERROR) << "leveldb error: " << st.ToString();
        return CONSUS_SERVER_ERROR;
    }
}

std::string
leveldb_datalayer :: data_key(const e::slice& table,
                              const e::slice& key,
                              uint64_t timestamp)
{
    std::string tmp;
    e::packer(&tmp)
        << table
        << e::pack_array<uint8_t>(key.data(), key.size())
        << timestamp;
    return tmp;
}

std::string
leveldb_datalayer :: lock_key(const e::slice& table,
                              const e::slice& key)
{
    std::string tmp;
    e::packer(&tmp)
        << e::slice("consus.lock")
        << table
        << e::pack_array<uint8_t>(key.data(), key.size());
    return tmp;
}
