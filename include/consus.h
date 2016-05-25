/* Copyright (c) 2015, Robert Escriva
 * All rights reserved.
 */

#ifndef consus_h_
#define consus_h_

/* C */
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/* consus_returncode occupies [6656, 6912) */
enum consus_returncode
{
    CONSUS_SUCCESS      = 6656,
    CONSUS_LESS_DURABLE = 6657,
    CONSUS_NOT_FOUND    = 6658,
    CONSUS_ABORTED      = 6659,

    /* persistent/programmatic errors */
    CONSUS_UNKNOWN_TABLE    = 6720,
    CONSUS_NONE_PENDING     = 6721,
    CONSUS_INVALID          = 6722,

    /* transient/operational errors */
    CONSUS_TIMEOUT      = 6784,
    CONSUS_INTERRUPTED  = 6785,
    CONSUS_SEE_ERRNO    = 6786,
    CONSUS_COORD_FAIL   = 6787,
    CONSUS_UNAVAILABLE  = 6788,
    CONSUS_SERVER_ERROR = 6789,

    /* this should never happen */
    CONSUS_INTERNAL     = 6910,
    CONSUS_GARBAGE      = 6911
};

struct consus_client;
struct consus_transaction;

struct consus_client* consus_create(const char* coordinator, uint16_t port);
struct consus_client* consus_create_conn_str(const char* conn_str);
void consus_destroy(struct consus_client* client);

int64_t consus_loop(struct consus_client* client, int timeout,
                    enum consus_returncode* status);
int64_t consus_wait(struct consus_client* client, int64_t id, int timeout,
                    enum consus_returncode* status);

int consus_poll_fd(struct consus_client* client);
int consus_block(struct consus_client* client, int timeout);

const char* consus_error_message(struct consus_client* client);
const char* consus_error_location(struct consus_client* client);
const char* consus_returncode_to_string(enum consus_returncode);

int64_t consus_begin_transaction(struct consus_client* client,
                                 enum consus_returncode* status,
                                 struct consus_transaction** xact);
int64_t consus_commit_transaction(struct consus_transaction* xact,
                                  enum consus_returncode* status);
int64_t consus_abort_transaction(struct consus_transaction* xact,
                                 enum consus_returncode* status);
int64_t consus_restart_transaction(struct consus_transaction* xact,
                                   enum consus_returncode* status);
void consus_destroy_transaction(struct consus_transaction* xact);

int64_t consus_get(struct consus_transaction* xact,
                   const char* table,
                   const char* key, size_t key_sz,
                   enum consus_returncode* status,
                   char** value, size_t* value_sz);
int64_t consus_put(struct consus_transaction* xact,
                   const char* table,
                   const char* key, size_t key_sz,
                   const char* value, size_t value_sz,
                   enum consus_returncode* status);

#ifdef __cplusplus
} /* extern "C" */
#endif /* __cplusplus */
#endif /* consus_h_ */
