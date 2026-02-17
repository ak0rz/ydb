PROGRAM(ws_system_bench)

SRCS(
    system_bench.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/core/workstealing
    ydb/library/actors/testlib
    library/cpp/getopt
)

END()
