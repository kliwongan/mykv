# mykv

A distributed key-value store written on top of LevelDB and the Raft consensus algorithm, intended for educational purposes, and is such is not meant for production usage.
Currently in progress; the objectives will be updated as the progression continues.

### Objectives

The initial goal was to write this without looking at production implementations, but after trying and failing on my own to write clean code that works nicely I decided to just reverse engineer and adapt [TiKV](https://github.com/tikv/raft-rs)'s event driven, state machine Raft node (which itself adapted from etcd iirc) and [Riteraft](https://github.com/PsiACE/riteraft)'s application layer, while (eventually) doing my own testing and benchmarking using Jepsen and Maelstrom.

The objectives of this project are to, in order of completion priority (highest to lowest):
- [x] Implement the top down "scaffolding" that encompasses the high level architecture, so I can start writing the actual features
- [X] Implement the fundamental Raft election algorithm
- [ ] Implement the distributed log properly, including proposals, configuration changes, etc
- [X] Run the application layer using Prost and gRPC
- [ ] Use a proper key-value store in the backend to persist the log entries
- [ ] Test the performance of the key-value store in-depth using basic tests and some more complex deterministic testing
- [ ] Benchmark and optimize using Jepsen and Maelstrom, ensuring linearizability and correctness
- [ ] Implement (chunked) snapshotting as outlined in the Raft paper

This is essentially my "learn Rust in-depth as an intermediate and might as well learn distributed systems too" project

### Usage

TBA

### Future additions

Due to the consumption of time, I chose not to implement features such as:

- Leadership transfer
- Membership/network configuration changes
- Pre-voting
- Other optimizations outlined in Diego Ongaro's PhD dissertation, which I did read some parts of, but are way outside the scope of this toy project

but the framework is there to extend the code to include these features. If I do implement these I'll do it without reference as the backbone is already there.

### Resources

If anyone reading wants to do a similar project, here are some useful resources for learning:

- [The raft paper](https://raft.github.io/raft.pdf)
- [The raft PhD thesis](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [Chinese article on the top level architecture of TiKV's implementation](https://blackredscarf.github.io/post/raft-build/#%E6%BB%B4%E7%AD%94%E4%B8%8E%E5%BF%83%E8%B7%B3)
- [A straightforward, single file Rust implementation in Go](https://eli.thegreenplace.net/2020/implementing-raft-part-1-elections/)
- [A student's guide to Raft by Jon Gjengset; very useful for debugging and stepping through non-obvious edge cases mentally and in testing](https://thesquareplanet.com/blog/students-guide-to-raft/)
- [How practical Raft consensus can be misunderstood, and how even etcd was missing some subtle details](https://blog.cloudflare.com/a-byzantine-failure-in-the-real-world/)
- [Interfacing with Jepsen with sled-rs](https://sled.rs/simulation.html)
- [CockroachDB on testing with Jepsen](https://www.cockroachlabs.com/blog/diy-jepsen-testing-cockroachdb/
)
