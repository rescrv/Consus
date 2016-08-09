// Copyright (c) 2016, Robert Escriva
// All rights reserved.

// consus
#include "kvs/table_key_pair.h"

using consus::table_key_pair;

table_key_pair :: table_key_pair()
    : table()
    , key()
{
}

table_key_pair :: table_key_pair(const e::slice& t, const e::slice& k)
    : table(t.str())
    , key(k.str())
{
}

table_key_pair :: ~table_key_pair() throw ()
{
}

bool
consus :: operator == (const table_key_pair& lhs, const table_key_pair& rhs)
{
    return lhs.table == rhs.table && lhs.key == rhs.key;
}
