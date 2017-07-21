spark-shell --packages org.nd4j:nd4j-native-platform:0.8.0

mvn package

scp -P 2222 target/dl4j-examples-scala-0.8-SNAPSHOT-jar-with-dependencies.jar root@localhost:/root/

