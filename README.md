# Graphsurge: Graph Analytics on View Collections using Differential Computations

![Continuous integration](https://github.com/dsg-uwaterloo/graphsurge/workflows/CI/badge.svg)

Graphsurge is a new system for performing analytical computations on multiple snapshots or _views_ 
of large-scale static property graphs. Graphsurge allows users to create _view collections_, a set 
of related views of a graph created by applying filter predicates on node and edge properties, and
run analytical computations on all the views of a collection efficiently.

Graphsurge is built on top of [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow) 
and [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow), which provides
two huge benefits:
* Differential Dataflow can incrementally maintain the results for any general computation, including
cyclic or iterative computations (which include many graph algorithms such as 
[Connected Components](https://en.wikipedia.org/wiki/Component_(graph_theory)). Analytical 
computations in Graphsurge are expressed using Differential operators and enables reusing 
computation results across the views of a collection instead of running computations from scratch
on each view. This results in huge savings on the total runtime.
* We use the Timely execution engine to seamlessly scale both the materialization of view
collections and running analytical computations to a distributed environment, similar to using 
other execution frameworks such as [Spark](https://spark.apache.org) or 
[Flink](https://flink.apache.org). 

Graphsurge stores view collections using a form of [delta encoding](https://en.wikipedia.org/wiki/Delta_encoding),
where the data for a view GV<sub>i</sub> represent its difference with the previous view GV<sub>i-1</sub>.
This representation can also be directly used as inputs to Differential Dataflow computations.

In _general_, the runtime of a black-box differential computation (such as the 
user-defined computations in Graphsurge) is correlated with the total number of diffs of a view 
collection. Graphsurge enabled 2 key optimizations based on this observation:
* **Collection Ordering**: The total number of diffs of a view collection depends on the order the
 views (similar views placed next to each other will generate less diffs) and we want to reorder 
 a given set of views to get the lowest number of diffs. This Collection Ordering Problem is related
 to the Consecutive Block Minimization Problem, which is NP-Hard!
 
  Graphsurge solved this problem using approximation algorithm from prior work to find better view orderings (resulting in up to 10x
  less diffs in our experiments).

* **Adaptive Collection Splitting**: Maintaining the computation results for any computation also
leads to overhead in Differential Dataflow, because it needs to check the entire history of a 
key to determine the effect of a new update. This overhead is especially large for cases where the 
number of diffs of a view are high, or for computations (like PageRank) which results 
in a large number of output changes even for small number of input updates. In such cases, it is 
faster to run the computation on a view from scratch instead of trying to reuse results from 
previous views.

  Graphsurge keeps track of the correlation between the number of the diffs and the 
  actual computation time when running differentially and also when rerunning from scratch. It uses
  a linear regression model to adaptively decide at runtime to split the view collection at the 
  point where rerunning from scratch is predicted to be faster than to continue running
  differentially.

More details on our techniques and experimental results can be found in [our paper](https://arxiv.org/abs/).

## Using Graphsurge

Graphsurge is written in [Rust](https://www.rust-lang.org). To run the Graphsurge cli, download and build 
the binary:

```bash
$ git clone https://github.com/dsg-uwaterloo/graphsurge && cd graphsurge
$ cargo build --release
$ ./target/bin/graphsurge
```

### Set the number of worker threads and process id:
```bash
graphsurge> SET THREADS 4 AND PROCESS_ID 0;
```

### Load a graph:
```bash
graphsurge> LOAD GRAPH WITH
    VERTICES FROM 'data/small_properties/vertices.txt' and
    EDGES FROM 'data/small_properties/edges.txt'
    COMMENT '#';
```
### Create a view collection:
```bash
graphsurge> CREATE VIEW COLLECTION Years WHERE
    [year <= 2000 and u.country = 'canada' and v.country = 'canada'],
    [year <= 2005 and u.country = 'canada' and v.country = 'canada'],
    [year <= 2010 and u.country = 'canada' and v.country = 'canada'];
```

### Run computations:
```bash
$ mkdir bfs_results
```
```bash
graphsurge> RUN COMPUTATION wcc ON COLLECTION Years SAVE RESULTS TO 'bfs_results';
```

### Running in a distributed environment:

To run Graphsurge on multiple machines, say on 2 hosts _server1_ and _server2_, start
Graphsurge and set the process ids:

```bash
# On server1
graphsurge> SET THREADS 32 AND PROCESS_ID 0;
```

```bash
# On server2
graphsurge> SET THREADS 32 AND PROCESS_ID 1;
```

Then run the same queries on both of them. Make sure that server1 and server2
can access each other at the specified port.

```bash
graphsurge> LOAD GRAPH WITH
    VERTICES FROM 'data/small_properties/vertices.txt' and
    EDGES FROM 'data/small_properties/edges.txt'
    COMMENT '#';
graphsurge> CREATE VIEW COLLECTION Years WHERE
    [year <= 2000 and u.country = 'canada' and v.country = 'canada'],
    [year <= 2005 and u.country = 'canada' and v.country = 'canada'],
    [year <= 2010 and u.country = 'canada' and v.country = 'canada']
    HOSTS 'server1:9000' 'server2:9000';
graphsurge> RUN ARRANGED_DIFFERENTIAL COMPUTATION wcc on COLLECTION Years
    HOSTS 'server1:9000' 'server2:9000';
```

The same process can be repeated for additional hosts machines.

### Writing new computations
Graphsurge already has [implementations](https://github.com/dsg-uwaterloo/graphsurge/blob/master/src/computations/builder.rs#L45)
for a set of common graph algorithms. New computations can be written using the [Analytics 
Computation API](https://github.com/dsg-uwaterloo/graphsurge/blob/master/gs_analytics_api/src).
You can see examples of how to use the API for [bfs](https://github.com/dsg-uwaterloo/graphsurge/tree/master/src/computations/bfs)
and [scc](https://github.com/dsg-uwaterloo/graphsurge/tree/master/src/computations/scc).
