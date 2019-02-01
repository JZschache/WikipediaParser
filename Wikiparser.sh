#!/bin/bash

java -Xmx14g -DentityExpansionLimit=0 -DtotalEntitySizeLimit=0 -Djdk.xml.totalEntitySizeLimit=0 -jar target/WikipediaParser-0.0.1-SNAPSHOT.jar
