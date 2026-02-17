PROGRAM(ws_ds_bench)

SRCS(
    ds_bench.cpp
)

PEERDIR(
    ydb/library/actors/core/workstealing
    library/cpp/getopt
)

END()
