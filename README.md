# GoalDetector
## Managing Big Data, January 2015

### Code overview

#### Kafka Spout
`output: "":String`

#### parser : ExtractDataFromTweetJSON
`output: "text":String, "lang":String, "time":String, "hashtags":List<String>`

#### checkgoal : ExtractGoalFromTweetData
`output: "time":Date, "hashtag":String, "match":Match, "score":Score`

#### summarizer : ReduceGoalStatements
`output: "time":Date, "match":Match, "score":Score`

#### sqlout : SQLOutputBolt
We set up a PHP script on a server, with an SQL database running as well, which handles the GET requests made by the SQLOutputBolt. The parameters of the GET requests are the data outputted from the summarizer which is then simply stored into the SQL database by the PHP script. We initially designed it this way in order to completely decouple the processing of the data and the display/usage of the generated information, however, due to time constraints we ended up only doing the analysis part of the project and much less so the visualisation of the generated information.

#### hdfsout : HdfsBolt
Our hdfsout bolt writes all data that comes from the summarizer to disk, more or less as redundancy as we also write the generated data to the SQL database using the sqlout bolt. The HdfsBolt writes to hdfs://ctit048:8020 using the settings as given in the Storm assignment of Managing Big Data. The only change we made was to change the output folter to our own user home folder and we changed the FileSizeRotationPolicy to 1KB, as the summarizer is not going to emit a lot of data, in the ideal case 1 tuple per scored goal.

### Deploying the program on the UT cluster
We wrote a script in order to automate the build-upload-run cycle, which is supposed to be run from the root of the Maven project (the same location as Maven's pom.xml). The script requires to be run with the student number as parameter and having automatic authentication (passwordless login) to the cluster set up already for the ssh/scp connections.

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
