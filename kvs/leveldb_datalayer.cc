// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>
#include <e/serialization.h>

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
        LOG(ERROR) << "could not open LevelDB: " << st.ToString();
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
    std::string tmp;
    e::packer(&tmp) << table << key << timestamp_le;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(leveldb::ReadOptions()));
    it->Seek(tmp);
    *timestamp = 0;
    *value = e::slice();
    *ref = NULL;

    if (!it->status().ok())
    {
        LOG(ERROR) << "LevelDB error: " << it->status().ToString();
        return CONSUS_INVALID;
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
    std::string tmp;
    e::packer(&tmp) << table << key << timestamp;
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
    std::string tmp;
    e::packer(&tmp) << table << key << timestamp;
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
