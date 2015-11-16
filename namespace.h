// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_namespace_h_
#define consus_namespace_h_

#define BEGIN_CONSUS_NAMESPACE \
    namespace consus __attribute__ ((visibility ("hidden"))) {

#define END_CONSUS_NAMESPACE }

#endif // consus_namespace_h_
