// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_client_server_selector_h_
#define consus_client_server_selector_h_

// STL
#include <vector>

// consus
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

class server_selector
{
    public:
        server_selector();
        ~server_selector() throw ();

    public:
        void set(const comm_id* ids, size_t ids_sz);
        comm_id next();

    private:
        std::vector<comm_id> m_ids;
        size_t m_consumed_idx;
};

END_CONSUS_NAMESPACE

#endif // consus_client_server_selector_h_
