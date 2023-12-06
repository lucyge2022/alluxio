#!/bin/bash

java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=60003 -cp /root/github/alluxio/conf/::/root/github/alluxio/assembly/server/target/alluxio-assembly-server-305-SNAPSHOT-jar-with-dependencies.jar -Dalluxio.logger.type=UCPSERVER_LOGGER -Dalluxio.home=/root/github/alluxio -Dalluxio.conf.dir=/root/github/alluxio/conf -Dalluxio.logs.dir=/root/github/alluxio/logs -Dalluxio.user.logs.dir=/root/github/alluxio/logs/user -Dlog4j.configuration=file:/root/github/alluxio/conf/log4j.properties -Dorg.apache.jasper.compiler.disablejsr199=true -Djava.net.preferIPv4Stack=true -Xmx4g -XX:MaxDirectMemorySize=4g alluxio.worker.ucx.UcpServer 10 /root/testfolder 1 2&>1 > /root/github/alluxio/logs/ucpserver.out &
