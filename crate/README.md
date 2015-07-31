## Quick Start

This section describes how to run YCSB benchmark on Crate.

### 1. Start Crate

Start Crate on a single machine or a cluster.

### 2. Set up YCSB

Clone YCSB repository:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB

To build the full distribution, with all database bindings run:

    mvn clean package

To build the Crate database binding run:

    mvn -pl com.yahoo.ycsb:crate-binding -am clean package

### 3. Crate configuration parameters

The following parameters are available:

  * `crate.hosts` - Comma separated list of Crate cluster hosts to connect to (default: `localhost:19301`)
  * `crate.shards` - The number of shards (default: `2`)
  * `crate.replicas` - The number of replicas (default: `1`)

### 5. Load data and run workload

Before running a workload, it has to be downloaded first:

    ./bin/ycsb load crate -s -P workloads/workloada -P crate/src/conf/crate.properties > outputLoad.txt

Then, the workload test can be run:

    ./bin/ycsb run crate -s -P workloads/workloada > outputRun.txt
