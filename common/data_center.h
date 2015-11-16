// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_common_data_center_h_
#define consus_common_data_center_h_

// consus
#include "namespace.h"
#include "common/ids.h"

BEGIN_CONSUS_NAMESPACE

class data_center
{
    public:
        data_center();
        data_center(data_center_id id, const std::string& name);
        data_center(const data_center& other);
        ~data_center() throw ();

    public:
        data_center_id id;
        std::string name;
};

std::ostream&
operator << (std::ostream& lhs, const data_center& rhs);

e::packer
operator << (e::packer lhs, const data_center& rhs);
e::unpacker
operator >> (e::unpacker lhs, data_center& rhs);
size_t
pack_size(const data_center& dc);

END_CONSUS_NAMESPACE

#endif // consus_common_data_center_h_
