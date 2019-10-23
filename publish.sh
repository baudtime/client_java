#!/bin/sh

mvn -U -B clean test && mvn -Pdelombok -B deploy -Dmaven.test.skip=true && mvn clean