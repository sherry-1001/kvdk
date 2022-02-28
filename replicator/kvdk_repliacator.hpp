/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once
#include "replicator/kvdk_wrapper.hpp"

namespace replication
{
    enum class EngineRole{
        MASTER,
        SLAVE,
        NO,
    };
}