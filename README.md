# flume-json-interceptor
Flume JSON Interceptor Plugin

## Required dependency

Download to `libext` directory:

    mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact=com.jayway.jsonpath:json-path:2.0.0:jar -Ddest=json-path-2.0.0.jar -Dtransitive=false
  
    mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact=net.minidev:json-smart:2.1.0:jar -Ddest=json-smart-2.1.0.jar -Dtransitive=false
  
    mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact=net.minidev:asm:1.0.2:jar -Ddest=asm-1.0.2.jar -Dtransitive=false