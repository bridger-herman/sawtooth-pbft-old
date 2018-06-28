# Sawtooth PBFT

Implementation of a practical Byzantine fault tolerant (PBFT) consensus
algorithm for Hyperledger Sawtooth.

The proposal for its inclusion in Sawtooth is located in [the associated RFC][pbft-rfc].

This is a prototype of the PBFT consensus algorithm - there is much work to be
done before it is ready to be deployed in a production context.

## Motivation
Sawtooth currently only supports PoET consensus (although other algorithms
like [Raft][raft] are currently under development). PoET is only crash fault
tolerant, so if any nodes in the network exhibit [Byzantine
behaviour][byzantine], it causes issues with consensus.


[pbft-rfc]: https://github.com/bridger-herman/sawtooth-rfcs/blob/pbft-consensus/text/0000-pbft-consensus.md
[raft]: https://github.com/hyperledger/sawtooth-raft
[byzantine]: https://en.wikipedia.org/wiki/Byzantine_fault_tolerance#Byzantine_Generals'_Problem
