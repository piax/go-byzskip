# sgsim
A simple simulator of Skip Graph.

## Build

```sh
$ go build
```

Put ``sgsim`` to somewhere in the $PATH.

## Usage

```
sgsim [OPTIONS]
  -a, --alpha int   the alphabet size of the membership vector (default 2)
  -n, --nodes int   number of nodes (default 100)
  -s, --seed int    give a random seed (default 3)
  -v, --verbose     verbose output
```

The ``sgsim`` program runs the followings:

1. all nodes are joined to the network.
2. send unicast messages from randomly selected nodes to randomly selected nodes.

## Example

Simulation with parameters {number of nodes=1000}:

```sh
sgsim -n 1000
```

The above command prints the following result:

```
INFO avg-match-hops: 8.478000
```

Note that the result is written to standard error output.
