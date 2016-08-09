// Copyright (c) 2016, Robert Escriva
// All rights reserved.

#ifndef consus_kvs_table_key_pair_h_
#define consus_kvs_table_key_pair_h_

// e
#include <e/compat.h>
#include <e/slice.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

struct table_key_pair
{
    table_key_pair();
    table_key_pair(const e::slice& table, const e::slice& key);
    ~table_key_pair() throw ();
    std::string table;
    std::string key;
};

bool
operator == (const table_key_pair& lhs, const table_key_pair& rhs);

END_CONSUS_NAMESPACE

BEGIN_E_COMPAT_NAMESPACE
template <>
struct hash<consus::table_key_pair>
{
    size_t operator()(const consus::table_key_pair& x) const
    {
        e::compat::hash<string> h;
        return h(x.table) ^ h(x.key);
    }
};
END_E_COMPAT_NAMESPACE

#endif // consus_kvs_table_key_pair_h_
