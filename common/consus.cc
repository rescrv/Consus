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

// consus
#include "common/consus.h"
#include "common/macros.h"

std::ostream&
consus :: operator << (std::ostream& lhs, const consus_returncode& rc)
{
    switch (rc)
    {
        STRINGIFY(CONSUS_SUCCESS);
        STRINGIFY(CONSUS_LESS_DURABLE);
        STRINGIFY(CONSUS_NOT_FOUND);
        STRINGIFY(CONSUS_ABORTED);
        STRINGIFY(CONSUS_COMMITTED);
        STRINGIFY(CONSUS_UNKNOWN_TABLE);
        STRINGIFY(CONSUS_NONE_PENDING);
        STRINGIFY(CONSUS_INVALID);
        STRINGIFY(CONSUS_TIMEOUT);
        STRINGIFY(CONSUS_INTERRUPTED);
        STRINGIFY(CONSUS_SEE_ERRNO);
        STRINGIFY(CONSUS_COORD_FAIL);
        STRINGIFY(CONSUS_UNAVAILABLE);
        STRINGIFY(CONSUS_SERVER_ERROR);
        STRINGIFY(CONSUS_INTERNAL);
        STRINGIFY(CONSUS_GARBAGE);
        default:
            lhs << "unknown consus_returncode";
    }

    return lhs;
}

e::packer
consus :: operator << (e::packer lhs, const consus_returncode& rhs)
{
    uint16_t mt = static_cast<uint16_t>(rhs);
    return lhs << mt;
}

e::unpacker
consus :: operator >> (e::unpacker lhs, consus_returncode& rhs)
{
    uint16_t mt;
    lhs = lhs >> mt;
    rhs = static_cast<consus_returncode>(mt);
    return lhs;
}

size_t
consus :: pack_size(const consus_returncode&)
{
    return sizeof(uint16_t);
}
