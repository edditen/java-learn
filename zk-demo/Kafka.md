
- start zookeeper (use as topic meta-server)
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```


- start kafka broker
```bash
bin/kafka-server-start.sh config/server.properties
```

- crate topic
```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```

- check topic list
```bash
bin/kafka-topics.sh --list --zookeeper localhost:2181
```

- producer send message test
```bash
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```

- consumer receive message from broker
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

