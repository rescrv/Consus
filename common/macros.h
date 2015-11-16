// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef kvs_common_macros_h_
#define kvs_common_macros_h_

#define XSTR(x) #x
#define STR(x) XSTR(x)
#define STRINGIFY(x) case (x): lhs << XSTR(x); break
#define STRINGIFYNS(ns, x) case (ns::x): lhs << XSTR(x); break
#define CSTRINGIFY(x) case (x): return XSTR(x);

#define _CONCAT(x, y) x ## y
#define CONCAT(x, y) _CONCAT(x, y)

#endif // kvs_common_macros_h_
