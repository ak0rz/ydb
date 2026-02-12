LIBRARY()

PEERDIR(
    library/cpp/threading/chunk_queue
)

END()

RECURSE_FOR_TESTS(
    ut
)
