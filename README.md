# GoalDetector
## Managing Big Data, January 2015

### Code overview


### Deploying the program on the UT cluster
We wrote a script in order to automate the build-upload-run cycle, which is supposed to be run from the root of the Maven project (the same location as Maven's pom.xml).

``` bash
#!/bin/bash -e 
mvn package
ssh $1@ctithead1.ewi.utwente.nl 'rm -rf storm*'
scp storm.properties target/storm-0.1.jar $1@ctithead1.ewi.utwente.nl:~
ssh $1@ctithead1.ewi.utwente.nl '/usr/lib/storm/bin/storm jar storm-0.1.jar nl.utwente.bigdata.GoalDetector storm.properties'
```

Running our Storm topology jar on the server takes a single argument, namely the filename of the configuration file. An example configuration file is given below, which is self-explanatory.
```
# Session id for database storage
session=3
# Run local or on cluster?
type=cluster
# Nimbus host
nimbus=ctit048
```

Once deployed, the Storm UI running on ctit048.ewi.utwente.nl:8080 gives us this image of our topology. When this image was captured the entire stream from the Kafka spout had already been processed hence all percentages indicate 0%, there are simply no more messages hence it cannot pass them on. It is noteworthy that the latency for every bolt, except for the SQLOutput is sub-millisecond. This can be expected as the SQLOutput is the only bolt having a connection to the outside world (in this case a http server, in which we can easily store and access our results without having to rely on the availability of the UT cluster.

![alt text](https://github.com/denniss17/mbd/blob/master/topology.png "Topology graph from the Storm UI interface on ctit048")
