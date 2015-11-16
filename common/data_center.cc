// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// e
#include <e/strescape.h>

// consus
#include "common/data_center.h"

using consus::data_center;

data_center :: data_center()
    : id()
    , name()
{
}

data_center :: data_center(data_center_id i, const std::string& n)
    : id(i)
    , name(n)
{
}

data_center :: data_center(const data_center& other)
    : id(other.id)
    , name(other.name)
{
}

data_center :: ~data_center() throw ()
{
}

std::ostream&
consus :: operator << (std::ostream& lhs, const data_center& rhs)
{
    return lhs << "data_center(id=" << rhs.id.get()
               << ", name=\"" << e::strescape(rhs.name) << "\")";
}

e::packer
consus :: operator << (e::packer lhs, const data_center& rhs)
{
    return lhs << rhs.id << e::slice(rhs.name);
}

e::unpacker
consus :: operator >> (e::unpacker lhs, data_center& rhs)
{
    e::slice name;
    lhs = lhs >> rhs.id >> name;
    rhs.name = name.str();
    return lhs;
}
