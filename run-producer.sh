#!/usr/bin/env bash

BROKERS_LIST="$@"
java -classpath ./target/hamster-1.0-SNAPSHOT-jar-with-dependencies.jar com.ashylin.hamster.client.Persist ${BROKERS_LIST}