#!/usr/bin/env bash

HOME_DIR=$1
PORT=$2
java -cp target/hamster-1.0-SNAPSHOT-jar-with-dependencies.jar com.ashylin.hamster.broker.BrokerRunner ${HOME_DIR} ${PORT}