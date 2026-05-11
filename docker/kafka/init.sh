/bin/kafka-topics --create \
  --topic user-notifs \
  --bootstrap-server localhost:29092 \
  --partitions 1 \
  --replication-factor 1