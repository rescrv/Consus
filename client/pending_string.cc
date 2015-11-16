// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// consus
#include "client/pending_string.h"

using consus::pending_string;

pending_string :: pending_string(const char* s)
    : pending(-1, NULL)
    , m_str(s)
{
}

pending_string :: pending_string(const std::string& s)
    : pending(-1, NULL)
    , m_str(s)
{
}

pending_string :: ~pending_string() throw ()
{
}

std::string
pending_string :: describe()
{
    return "pending_string(" + m_str + ")";
}
