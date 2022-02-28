/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "include/kvdk/engine.hpp"

namespace replication {
class EngineWrapper : public std::enable_shared_from_this<EngineWrapper> {
 public:
  uint64_t LatestSequenceNumber() {}
  EngineWrapper(const std::string& engine_name,
                std::shared_ptr<KVDK_NAMESPACE::Engine> engine, KVDK_NAMESPACE::Configs configs)
      : engine_name_(engine_name), engine_(engine), configs_(configs) {}

 private:
  std::shared_ptr<KVDK_NAMESPACE::Engine> engine_;
  const std::string engine_name_;
  KVDK_NAMESPACE::Configs configs_;
};

}  // namespace replication