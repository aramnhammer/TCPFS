# TCPFS
A custom file server implementation with a focus on traceability that implementes its own transfer protocol.

## features
- track `bucket` sizes as new files are added and removed
- allow application of retention policies to `buckets` where a background thread will monitor allocations and clean up based on creation time or another determinent, - not implemented
- generate client stubs: python, rust, C - not implemented
- SSL/TLS - not implemented