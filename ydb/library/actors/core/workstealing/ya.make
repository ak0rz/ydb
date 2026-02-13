LIBRARY()

IF (OS_LINUX)
    SRCS(
        topology.cpp
    )
ENDIF()

PEERDIR(
    library/cpp/threading/chunk_queue
)

END()

RECURSE_FOR_TESTS(
    ut
)
