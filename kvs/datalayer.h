// Copyright (c) 2016, Robert Escriva
// All rights reserved.

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
