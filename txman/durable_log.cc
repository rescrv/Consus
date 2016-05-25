// Copyright (c) 2015, Robert Escriva
// All rights reserved.

// C
#include <assert.h>
#include <string.h>

// POSIX
#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>

// STL
#include <algorithm>
#include <vector>

// e
#include <e/endian.h>
#include <e/guard.h>
#include <e/serialization.h>
#include <e/varint.h>

// consus
#include "common/crc32c.h"
#include "txman/durable_log.h"

using consus::durable_log;

#define RECORD_HEADER_SIZE (2 * sizeof(uint64_t))

static void
encode_header(uint64_t recno, uint64_t size, unsigned char* header)
{
    e::pack64be(recno, header);
    e::pack64be(size, header + sizeof(uint64_t));
}

struct durable_log :: segment
{
    segment(po6::threads::mutex* mtx, int x)
        : fd(x)
        , offset_next_write(0)
        , offset_last_fsync(0)
        , recno_last_write(0)
        , recno_last_fsync(0)
        , ongoing_writes(0)
        , done_writing(mtx)
        , syncing(false)
    {
    }
    po6::io::fd fd;
    uint64_t offset_next_write;
    uint64_t offset_last_fsync;
    uint64_t recno_last_write;
    uint64_t recno_last_fsync;
    int32_t ongoing_writes;
    po6::threads::cond done_writing;
    bool syncing;
};

durable_log :: durable_log()
    : m_path()
    , m_dir()
    , m_lockfile()
    , m_mtx()
    , m_cond(&m_mtx)
    , m_flush(po6::threads::make_obj_func(&durable_log::flush, this))
    , m_error(0)
    , m_wakeup(false)
    , m_next_entry(1)
    , m_segment_a(NULL)
    , m_segment_b(NULL)
{
    m_flush.start();
}

durable_log :: ~durable_log() throw ()
{
    close();
    m_flush.join();

    if (m_segment_a)
    {
        delete m_segment_a;
    }

    if (m_segment_b)
    {
        delete m_segment_b;
    }
}

bool
durable_log :: open(const std::string& dir)
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_path = dir;
    struct stat st;
    int ret = stat(m_path.c_str(), &st);

    if (ret < 0 && errno == ENOENT)
    {
        if (mkdir(m_path.c_str(), S_IRWXU) < 0)
        {
            m_error = errno;
            return false;
        }

        ret = stat(m_path.c_str(), &st);
    }

    if (ret < 0)
    {
        m_error = errno;
        return false;
    }
    else if (!S_ISDIR(st.st_mode))
    {
        m_error = errno = ENOTDIR;
        return false;
    }

    m_dir = ::open(m_path.c_str(), O_RDONLY);

    if (m_dir.get() < 0)
    {
        m_error = errno;
        return false;
    }

    int fd = openat(m_dir.get(), "LOCK", O_RDWR|O_CREAT, S_IRUSR|S_IWUSR);

    if (!m_lockfile.lock(fd))
    {
        m_error = errno;
        return false;
    }

    // XXX do rotation on these and don't overwrite the old
    int file_a = openat(m_dir.get(), "file_a", O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
    int file_b = openat(m_dir.get(), "file_b", O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);

    if (file_a < 0 || file_b < 0)
    {
        ::close(file_a);
        ::close(file_b);
        return false;
    }

    m_segment_a = new segment(&m_mtx, file_a);
    m_segment_b = new segment(&m_mtx, file_b);
    return true;
}

void
durable_log :: close()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_error = -1;
    m_cond.broadcast();
}

int64_t
durable_log :: append(const char* entry, size_t entry_sz)
{
    return append(reinterpret_cast<const unsigned char*>(entry), entry_sz);
}

int64_t
durable_log :: append(const unsigned char* entry, size_t entry_sz)
{
    unsigned char header[RECORD_HEADER_SIZE];
    segment* seg;
    uint64_t offset;
    uint64_t recno;

    {
        po6::threads::mutex::hold hold(&m_mtx);

        if (m_error)
        {
            errno = m_error;
            return -1;
        }

        recno = m_next_entry;
        ++m_next_entry;
        seg = select_segment_write();
        assert(seg);
        assert(!seg->syncing);
        offset = seg->offset_next_write;
        seg->offset_next_write += RECORD_HEADER_SIZE + entry_sz + sizeof(uint32_t);
        seg->recno_last_write = recno;
        ++seg->ongoing_writes;
    }

    encode_header(recno, entry_sz, header);
    uint32_t crc = 0;
    crc = crc32c(crc, header, RECORD_HEADER_SIZE);
    crc = crc32c(crc, entry, entry_sz);
    unsigned char crcbuf[sizeof(uint32_t)];
    e::pack32be(crc, crcbuf);

    if (pwrite(seg->fd.get(), header, RECORD_HEADER_SIZE, offset) < 0 ||
        pwrite(seg->fd.get(), entry, entry_sz, offset + RECORD_HEADER_SIZE) < 0 ||
        pwrite(seg->fd.get(), crcbuf, sizeof(uint32_t), offset + RECORD_HEADER_SIZE + entry_sz) < 0)
    {
        int e = errno;
        po6::threads::mutex::hold hold(&m_mtx);
        m_error = e;
        return -1;
    }

    po6::threads::mutex::hold hold(&m_mtx);
    --seg->ongoing_writes;

    if (seg->ongoing_writes == 0)
    {
        seg->done_writing.broadcast();
        m_cond.broadcast();
    }

    return recno;
}

int64_t
durable_log :: durable()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return durable_lock_held_elsewhere();
}

int64_t
durable_log :: wait(int64_t prev_ub)
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (true)
    {
        int64_t x = durable_lock_held_elsewhere();

        if (m_error == 0 && x <= prev_ub && !m_wakeup)
        {
            m_cond.wait();
        }
        else
        {
            m_wakeup = false;
            return x;
        }
    }
}

void
durable_log :: wake()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_wakeup = true;
    m_cond.broadcast();
}

int
durable_log :: error()
{
    po6::threads::mutex::hold hold(&m_mtx);
    return m_error;
}

void
durable_log :: flush()
{
    sigset_t ss;

    if (sigfillset(&ss) < 0 ||
        pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        int err = errno;
        po6::threads::mutex::hold hold(&m_mtx);
        m_error = err;
        return;
    }

    while (true)
    {
        uint64_t offset_saved;
        uint64_t recno_saved;
        segment* seg;

        {
            po6::threads::mutex::hold hold(&m_mtx);

            while (m_error == 0 &&
                   !(seg = select_segment_fsync()))
            {
                m_cond.wait();
            }

            if (m_error != 0)
            {
                break;
            }

            seg->syncing = true;

            while (seg->ongoing_writes > 0)
            {
                seg->done_writing.wait();
            }

            offset_saved = seg->offset_next_write;
            recno_saved = seg->recno_last_write;
        }

        if (fsync(seg->fd.get()) < 0)
        {
            int e = errno;
            po6::threads::mutex::hold hold(&m_mtx);
            m_error = e;
        }

        {
            po6::threads::mutex::hold hold(&m_mtx);
            seg->syncing = false;
            seg->offset_last_fsync = offset_saved;
            seg->recno_last_fsync = recno_saved;
            m_cond.broadcast();
        }
    }
}

durable_log::segment*
durable_log :: select_segment_write()
{
    segment* a = m_segment_a;
    segment* b = m_segment_b;
    assert(a->offset_next_write >= a->offset_last_fsync);
    assert(b->offset_next_write >= b->offset_last_fsync);
    const uint64_t a_unflushed = a->offset_next_write - a->offset_last_fsync;
    const uint64_t b_unflushed = b->offset_next_write - b->offset_last_fsync;

    if (a_unflushed < b_unflushed && !a->syncing)
    {
        return a;
    }
    else if (a_unflushed > b_unflushed && !b->syncing)
    {
        return b;
    }
    else if (!a->syncing)
    {
        return a;
    }
    else if (!b->syncing)
    {
        return b;
    }
    else
    {
        return NULL;
    }
}

durable_log::segment*
durable_log :: select_segment_fsync()
{
    segment* a = m_segment_a;
    segment* b = m_segment_b;

    if (!a || !b)
    {
        return NULL;
    }

    assert(!a->syncing);
    assert(!b->syncing);
    assert(a->offset_next_write >= a->offset_last_fsync);
    assert(b->offset_next_write >= b->offset_last_fsync);
    const uint64_t a_unflushed = a->offset_next_write - a->offset_last_fsync;
    const uint64_t b_unflushed = b->offset_next_write - b->offset_last_fsync;

    if (a_unflushed < b_unflushed)
    {
        return b;
    }
    else if (a_unflushed > b_unflushed)
    {
        return a;
    }
    else if (a_unflushed > 0)
    {
        return a;
    }
    else if (b_unflushed > 0)
    {
        return b;
    }
    else
    {
        return NULL;
    }
}

int64_t
durable_log :: durable_lock_held_elsewhere()
{
    segment* a = m_segment_a;
    segment* b = m_segment_b;
    assert(a->offset_next_write >= a->offset_last_fsync);
    assert(b->offset_next_write >= b->offset_last_fsync);

    if (a->recno_last_fsync > b->recno_last_fsync)
    {
        std::swap(a, b);
    }

    if (a->offset_next_write - a->offset_last_fsync > 0)
    {
        return a->recno_last_fsync + 1;
    }

    return b->recno_last_fsync + 1;
}
