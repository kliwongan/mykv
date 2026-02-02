# mykv

A distributed key-value store written on top of LevelDB and the Raft consensus algorithm, intended for educational purposes, and is such is not meant for production usage.
Currently in progress; the objectives will be updated as the progression continues.

### Objectives

The initial goal was to write this without looking at production implementations, but after trying and failing on my own to write clean code that works nicely I decided to just reverse engineer and adapt [TiKV](https://github.com/tikv/raft-rs)'s event driven, state machine Raft node (which itself adapted from etcd iirc) and [Riteraft](https://github.com/PsiACE/riteraft)'s application layer, while doing my own testing and benchmarking using Jepsen and Maelstrom.

The objectives of this project were to, in order of completion priority (highest to lowest):
1. Implement the fundamental Raft election algorithm
2. Implement the distributed log properly, including proposals, configuration changes, etc
3. Run the application layer using Prost and gRPC
4. Use a proper key-value store in the backend to persist the log entries
5. Test the performance of the key-value store in-depth using basic tests and some more complex deterministic testing
6. Benchmark and optimize using Jepsen and Maelstrom, ensuring linearizability and correctness
7. Implement (chunked) snapshotting as outlined in the Raft paper 

This was basically my "learn Rust in-depth as an intermediate and might as well learn distributed systems too" project

### Usage



### Future additions

Due to the consumption of time, I chose not to implement features such as:

- Leadership transfer
- Membership/network configuration changes
- Pre-voting
- Other optimizations outlined in Diego Ongaro's PhD dissertation, which I did read some parts of, but are way outside the scope of this toy project

but the framework is there to extend the code to include these features. If I do implement these I'll do it without reference as the backbone is already there.

### Resources

If anyone reading wants to do a similar project, here are some useful resources for learning:

