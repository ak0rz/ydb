UNITTEST_FOR(ydb/library/actors/core/workstealing)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    SPLIT_FACTOR(10)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/testlib
)

SRCS(
    activation_router_ut.cpp
    cpu_manager_ws_ut.cpp
    executor_pool_ws_integration_ut.cpp
    executor_pool_ws_ut.cpp
    harmonizer_adapter_ut.cpp
    mpmc_unbounded_queue_ut.cpp
    thread_driver_ut.cpp
    topology_ut.cpp
    ws_adaptive_scaler_ut.cpp
    ws_executor_context_ut.cpp
    ws_poll_ut.cpp
    ws_slot_ut.cpp
    ws_stress_ut.cpp
)

END()
