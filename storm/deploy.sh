#!/bin/bash -e
mvn package
ssh $1@ctithead1.ewi.utwente.nl 'rm -rf storm*'
scp storm.properties target/storm-0.1.jar $1@ctithead1.ewi.utwente.nl:~
ssh $1@ctithead1.ewi.utwente.nl '/usr/lib/storm/bin/storm jar storm-0.1.jar nl.utwente.bigdata.GoalDetector storm.properties'
