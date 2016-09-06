/* Copyright (c) 2015, Robert Escriva
 * All rights reserved.
 */

#ifndef consus_admin_h_
#define consus_admin_h_

/* consus */
#include <consus.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

int consus_admin_create_data_center(struct consus_client* client, const char* name,
                                    enum consus_returncode* status);
int consus_admin_set_default_data_center(struct consus_client* client, const char* name,
                                         enum consus_returncode* status);

struct consus_availability_requirements
{
    unsigned txmans;
    unsigned txman_groups;
    unsigned kvss;
    int stable;
};

int consus_admin_availability_check(struct consus_client* client,
                                    struct consus_availability_requirements* reqs,
                                    int timeout,
                                    enum consus_returncode* status);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* consus_admin_h_ */
