// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_ids_h_
#define consus_common_ids_h_

// C
#include <stdint.h>

// C++
#include <iostream>

// e
#include <e/buffer.h>

// consus
#include "namespace.h"

// An ID is a simple wrapper around uint64_t in order to prevent devs from
// accidently using one type of ID as another.

#define OPERATOR(TYPE, OP) \
    inline bool \
    operator OP (const TYPE ## _id& lhs, const TYPE ## _id& rhs) \
    { \
        return lhs.get() OP rhs.get(); \
    }
#define CREATE_ID(TYPE) \
    BEGIN_CONSUS_NAMESPACE \
    class TYPE ## _id \
    { \
        public: \
            static uint64_t hash(const TYPE ## _id& x) { return x.get(); } \
            TYPE ## _id() : m_id(0) {} \
            explicit TYPE ## _id(uint64_t id) : m_id(id) {} \
        public: \
            uint64_t get() const { return m_id; } \
        private: \
            uint64_t m_id; \
    }; \
    std::ostream& \
    operator << (std::ostream& lhs, const TYPE ## _id& rhs); \
    inline size_t \
    pack_size(const TYPE ## _id&) \
    { \
        return sizeof(uint64_t); \
    } \
    e::packer \
    operator << (e::packer pa, const TYPE ## _id& rhs); \
    e::unpacker \
    operator >> (e::unpacker up, TYPE ## _id& rhs); \
    OPERATOR(TYPE, <) \
    OPERATOR(TYPE, <=) \
    OPERATOR(TYPE, ==) \
    OPERATOR(TYPE, !=) \
    OPERATOR(TYPE, >=) \
    OPERATOR(TYPE, >) \
    END_CONSUS_NAMESPACE \
    BEGIN_E_COMPAT_NAMESPACE \
    template <> \
    struct hash<consus::TYPE ## _id> \
    { \
        size_t operator()(consus::TYPE ## _id x) const \
        { \
            e::compat::hash<uint64_t> h; \
            return h(x.get()); \
        } \
    }; \
    END_E_COMPAT_NAMESPACE

CREATE_ID(abstract)
CREATE_ID(cluster)
CREATE_ID(version)
CREATE_ID(comm)
CREATE_ID(paxos_group)
CREATE_ID(data_center)
CREATE_ID(partition)

#undef OPERATOR
#undef CREATE_ID
#endif // consus_common_ids_h_
