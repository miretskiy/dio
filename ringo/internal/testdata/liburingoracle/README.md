# liburing public headers

This directory contains the installed public-header set generated from the
upstream `liburing-2.15` tag. DIO uses it only as a test oracle for its private
io_uring ABI; no liburing implementation is built or linked.

To update the snapshot, extract the new upstream tag on Linux, run
`./configure`, and copy the public headers installed by `make -C src install`.
Keep `LICENSE` in sync with the selected release.
