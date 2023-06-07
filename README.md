# Fault-Detection-in-Edge-Storage

The workflow is as such:
We use Netdata to monitor one or more nodes, and by leveraging it's API we use the timeseries to detect faults in the system.

## In this version

we have two nodes, a Server running a MariaDB database and a Client, querying that database.

We have a Netdata Agent on each system.

The file `dbtest.py` is then run on the Client. It has embedded some bugs that produce database "faults" (wrong queries). The goal of the `watcher.py` is to be able to determine these "anomalies" or "faults".
