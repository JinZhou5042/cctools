# Remaining research decisions

This round closes the kernel-only application gap, exercises native grouping with compatible semantics, adds five-block physical-host scaling, and separates initialization and instrumentation effects. The measured contribution is the ownership and ordering of dispatch, preparation, and execution. Fixed admission matching feedback is counterevidence to an adaptive-control novelty claim.

The next changes should follow the negative results:

1. **Prepared-node placement:** implement a semantically matched preparation-before-assignment control. Preserve individual results, retries and durable sinks. Eager preparation alone does not reproduce it.
2. **Data-heavy scaling:** use the retained 1-MiB chain plateau to distinguish controller CPU, manager serialization and locality cost before changing architecture. Measure sender bytes and prepared physical bytes directly. Do not infer total traffic from manager counters.
3. **Reserved 16+ nodes:** retain five randomized blocks and actual CPU/host verification. Current 1/2/4/8-host allocations provide CPU shares, not whole-node exclusivity. Grid Engine access and whole-machine Condor attempts did not yield an eligible allocation in this session.
4. **Numerical portability:** validate the reference implementation on each execution platform. The primary comparison remains unchanged. The independent alternate-platform reference reproduces the two-event difference, and the artifact now builds platform-specific manifests without using runtime outputs as an oracle.
5. **Production application scope:** the complete ATLAS educational analysis is substantially stronger evidence than kernels, but is not a production collaboration deployment or distributed ROOT ingestion study.

A stronger paper does not require every variant to win. Keep the cold-start penalty, modest application ratio, data-path plateau, and fixed/elastic similarity visible. Further architecture changes should target a measured bottleneck and earn their own before/after evidence.

No submission or publication has been performed.

Diagnostic paths containing `intel` retain an initial naming mistake. The alternate platform is recorded as AMD EPYC 9334; the observed numerical difference is not attributed to CPU vendor.
