UNITTEST_FOR(ydb/library/actors/core/workstealing)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(ya:fat)
    TIMEOUT(600)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    injection_queue_ut.cpp
    local_deque_ut.cpp
    topology_ut.cpp
)

END()
