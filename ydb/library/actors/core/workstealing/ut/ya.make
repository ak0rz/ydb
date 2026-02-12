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
    local_deque_ut.cpp
)

END()
