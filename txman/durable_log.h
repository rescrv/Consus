// Copyright (c) 2015-2016, Robert Escriva, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of Consus nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

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
