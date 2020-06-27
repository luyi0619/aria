//
// Created by Yi Lu on 9/10/18.
//

#pragma once

namespace aria {

enum class ExecutorStatus {
  START,
  CLEANUP,
  C_PHASE,
  S_PHASE,
  Analysis,
  Execute,
  Aria_READ,
  Aria_COMMIT,
  AriaFB_READ,
  AriaFB_COMMIT,
  AriaFB_Fallback_Prepare,
  AriaFB_Fallback,
  Bohm_Analysis,
  Bohm_Insert,
  Bohm_Execute,
  Bohm_GC,
  Pwv_Analysis,
  Pwv_Execute,
  STOP,
  EXIT
};

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

} // namespace aria
