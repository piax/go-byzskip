# bssim
A simulator of ByzSkip.

## Build

```sh
$ go build
```

Put ``bssim`` to somewhere in the $PATH.

## Usage

```
bssim [OPTIONS]
```

The ``bssim`` program runs the following by default.

1. all nodes are joined to the network.
2. send unicast messages from randomly selected nodes to randomly selected nodes.

If ``-p (--enable-pollution-calc)`` is true, 2. (unicast) is not executed but the ratio of the routing table entries that prevented pollution of the adversary is calculated.

The option ``-e (--exp-type)`` specifies the unicast experiment type in 2. If ``uni-max`` is specified, 500 nodes are randomly selected as the destinations from 100 randomly selected nodes to calculate average maximum hops/msgs (500 and 100 are hard-coded).

The option ``-u (--attack-type)`` specifies the attack type. ``cea`` and ``ara`` means Colluded Ecplise Attack (CEA) and Adversarial Routing Attack (ARA), respecrively.
The options ``iter`` and ``recur`` (in the options ``-j (--join-type)`` and ``-r (--unicast-routing-type)``) respectively means iterative routing and recursive routing.

## Example

Simulation with parameters {number of nodes=1000, k=6, under failure=ARA}:

```sh
bssim -n 1000 -k 6 -u ara
```

The above command prints the following result:

```
INFO 2022-07-20 16:55:09.144432 +0900 JST m=+0.241089251 10 percent of 1000 nodes
INFO 2022-07-20 16:55:09.784973 +0900 JST m=+0.881363584 20 percent of 1000 nodes
INFO 2022-07-20 16:55:10.72245 +0900 JST m=+1.818474584 30 percent of 1000 nodes
INFO 2022-07-20 16:55:11.893608 +0900 JST m=+2.989205043 40 percent of 1000 nodes
INFO 2022-07-20 16:55:13.385935 +0900 JST m=+4.481031334 50 percent of 1000 nodes
INFO 2022-07-20 16:55:15.086112 +0900 JST m=+6.180690668 60 percent of 1000 nodes
INFO 2022-07-20 16:55:16.836163 +0900 JST m=+7.930262918 70 percent of 1000 nodes
INFO 2022-07-20 16:55:18.848942 +0900 JST m=+9.942554209 80 percent of 1000 nodes
INFO 2022-07-20 16:55:21.052446 +0900 JST m=+12.145589751 90 percent of 1000 nodes
INFO avg-sum-candidates: 1000 6 2 0.20 ara iter recur:opt 3258.550551
INFO avg-join-msgs: 1000 6 2 0.20 ara iter recur:opt 194.786787
INFO faulty-entry-ratio: 1000 6 2 0.20 ara iter recur:opt 15522 69968 0.221844
INFO avg-match-hops: 1000 6 2 0.20 ara iter recur:opt 2.306000
INFO avg-msgs: 1000 6 2 0.20 ara iter recur:opt 99.006000
INFO success-ratio: 1000 6 2 0.20 ara iter recur:opt 1.000000
INFO avg-table-size: 1000 6 2 0.20 ara iter recur:opt 90.048906
```

The progress of join process is printed since the it is a time consuming task.

The each output line shows the following parameters/values.
```
<number of nodes> <k> <alpha> <failure ratio> <attack> <join type> <unicast routing time> <value>
```

The type of ``<value>`` depends on the item.
The values in faulty-entry-ratio line is ``<number of all faulty entries> <number of all entries> and <ratio of faulty entries>``.
You can pick the desired results by ``grep`` or something.
Note that the results are written to standard error output.
