# kadsim
A simulator of S/Kademlia.

## Build

```sh
$ go build
```

Put ``kadsim`` to somewhere in the $PATH.

## Usage

```
kadsim [OPTIONS]
  -a, --alpha int             the parallelism parameter (default 3)
  -u, --attack-type string    runs under attacks {none|ara|cea|stop} (default "cea")
  -k, --bucket-size int       the bucket size parameter (default 16)
  -d, --disjoint-paths int    the disjoint path parameter (default 1)
  -e, --exp-type string       uni|uni-max (default "uni")
  -f, --failure-ratio float   failure ratio (default 0.2)
  -n, --nodes int             number of nodes (default 100)
  -s, --seed int              give a random seed (default 1)
  -t, --trials int            number of search trials (-1 means same as nodes) (default -1)
  -v, --verbose               verbose output
```

The ``kadsim`` program runs the followings:

1. all nodes are joined to the network.
2. send unicast messages from randomly selected nodes to randomly selected nodes.

The option ``-e (--exp-type)`` specifies the unicast experiment type in 2. If ``uni-max`` is specified, 500 nodes are randomly selected as the destinations from 100 randomly selected nodes to calculate average maximum hops/msgs (500 and 100 are hard-coded).

The option ``-u (--attack-type)`` specifies the attack type. ``cea`` and ``ara`` means Colluded Ecplise Attack (CEA) and Adversarial Routing Attack (ARA), respecrively.

## Example

Simulation with parameters {number of nodes=1000, k=16, d=8, under failure=ARA}:

```sh
kadsim -n 1000 -k 16 -d 8 -u ara
```

The above command prints the following result:

```
INFO 2022-07-21 08:57:23.370653 +0900 JST m=+0.060128501 10 percent of 1000 nodes
INFO 2022-07-21 08:57:23.422685 +0900 JST m=+0.112163751 20 percent of 1000 nodes
INFO 2022-07-21 08:57:23.484206 +0900 JST m=+0.173688751 30 percent of 1000 nodes
INFO 2022-07-21 08:57:23.543401 +0900 JST m=+0.232886793 40 percent of 1000 nodes
INFO 2022-07-21 08:57:23.606403 +0900 JST m=+0.295893418 50 percent of 1000 nodes
INFO 2022-07-21 08:57:23.675555 +0900 JST m=+0.365049459 60 percent of 1000 nodes
INFO 2022-07-21 08:57:23.742069 +0900 JST m=+0.431566918 70 percent of 1000 nodes
INFO 2022-07-21 08:57:23.814518 +0900 JST m=+0.504020334 80 percent of 1000 nodes
INFO 2022-07-21 08:57:23.885738 +0900 JST m=+0.575245001 90 percent of 1000 nodes
INFO avg-join-msgs: 1000 16 8 3 0.20 ara 890.850000
INFO faulty-entry-ratio: 1000 16 8 3 0.20 ara 80348 388878 0.206615
INFO avg-paths-length: 1000 16 8 3 0.20 ara 9.148000
INFO avg-match-hops: 1000 16 8 3 0.20 ara 2.158475
INFO avg-msgs: 1000 16 8 3 0.20 ara 123.164000
INFO success-ratio: 1000 16 8 3 0.20 ara 0.997000
INFO avg-table-size: 1000 16 8 3 0.20 ara 110.021000
```

The progress of join process is printed since the it is a time consuming task.

The each output line shows the following parameters/values.
```
<number of nodes> <k> <d> <failure ratio> <attack> <value>
```

The type of ``<value>`` depends on the item.
The value of 
The values in faulty-entry-ratio line is ``<number of all faulty entries> <number of all entries> and <ratio of faulty entries>``.
You can pick the desired results by ``grep`` or something.
Note that the results are written to standard error output.
