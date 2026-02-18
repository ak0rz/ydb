LIBRARY()

SRCS(
    activation_router.cpp
    executor_pool_ws.cpp
    harmonizer_adapter.cpp
    thread_driver.cpp
    topology.cpp
    ws_executor_context.cpp
    ws_poll.cpp
    ws_slot.cpp
)

PEERDIR(
    ydb/library/actors/util
    library/cpp/lwtrace
)

END()

RECURSE(
    bench/system
)

RECURSE_FOR_TESTS(
    ut
)
