// Copyright (c) 2015, Robert Escriva
// All rights reserved.

#ifndef consus_txman_log_h_
#define consus_txman_log_h_

// C
#include <stdint.h>

// STL
#include <memory>
#include <string>
#include <vector>

// po6
#include <po6/io/fd.h>
#include <po6/threads/mutex.h>
#include <po6/threads/cond.h>
#include <po6/threads/thread.h>

// e
#include <e/lockfile.h>

// consus
#include "namespace.h"

BEGIN_CONSUS_NAMESPACE

class durable_log
{
    public:
        durable_log();
        ~durable_log() throw ();

    public:
        bool open(const std::string& dir);
        void close();
        int64_t append(const char* entry, size_t entry_sz);
        int64_t append(const unsigned char* entry, size_t entry_sz);
        int64_t replay(void (*f)(void*, const unsigned char*, size_t), void* p);
        int64_t durable();
        int64_t wait(int64_t prev_ub);
        void wake();
        int error();

    private:
        class segment;
        void flush();
        segment* select_segment_write();
        segment* select_segment_fsync();
        int64_t durable_lock_held_elsewhere();

    private:
        std::string m_path;
        po6::io::fd m_dir;
        e::lockfile m_lockfile;
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cond;
        po6::threads::thread m_flush;
        int m_error;
        bool m_wakeup;
        uint64_t m_next_entry;
        segment* m_segment_a;
        segment* m_segment_b;

    private:
        durable_log(const durable_log&);
        durable_log& operator = (const durable_log&);
};

END_CONSUS_NAMESPACE

#endif // consus_txman_log_h_
