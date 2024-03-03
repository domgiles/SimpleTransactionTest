## Simple Oracle Transaction Tester

### Pre-requisites
You'll need a JVM of version 11 or higher. OpenJDK or Oracle works fine
If you want to use the oci connectivity you'll need to have an Oracle Client installed (Instant or full fat). The ivy client used will download 19.21 jar files. If you want to use a different client make sure you edit ```ivy.xml```  

The workload will insert rows and retrieve them from the Swingbench ```CUSTOMERS``` Table. In the initial build it it expected fro this table to be there. In later builds I'll automoatically create the table.


### Install

I find it useful to test transaction performance to an Oracle Database. This utility will connect to an Oracle Database with a user specified number of threads and report the time taken to connect, and the number of transactions completed in the specified time (specified by ```-rt```) simple transaction workload. 

You can build the jar file with the following command
```shell
./build.sh
```
This should create a jar file called ```transactiontest.jar```. You can then invoke it using a command similar to 
```shell
java -jar transactiontest.jar
```
This should output
```shell
ERROR : Missing required options: u, p, cs
usage: parameters:
 -ac                   use application continuity
 -cf <zipfile>         credentials file in zip format
 -cs <connectstring>   connect string
 -ct <threadcount>     pds or ods
 -debug                turn on debugging. Written to standard out
 -dt <driver_type>     Driver Type [thin,oci]
 -o <arg>              output : valid values are stdout,csv
 -p <password>         password
 -rt <runtime>         runtime of test
 -tc <output>          thread count, defaults to 1
 -tt <thinktime>       Think Time (milliseconds)
 -u <username>         username
```
I believe the options should be self-explanatory however a simple example is shown below
```shell
java -jar transactiontest.jar -u soe -p soe -cs //localhost/soe -rt 0:00.5 -tc 20 -tt 50 -ct pds -ac                                                                                                                                        255 ↵
Starting Simple Transaction Test
Using Oracle Driver version 19.21.0.0.0, Built on Wed_Aug_02_04:16:16_PDT_2023
Connecting using a thin driver
Timer fired. Finishing Benchmark 
Connected 20 threads, Average connect time = 751.75ms, Average run time = 4279.60ms, Total Transactions Completed = 1465
```