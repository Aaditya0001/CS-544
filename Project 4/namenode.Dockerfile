# Use p4-hdfs as the base image
FROM p4-hdfs

# Run commands to format the NameNode and start it with specified configurations
CMD ["sh", "-c", "hdfs namenode -format && hdfs namenode -D dfs.namenode.stale.datanode.interval=10000 -D dfs.namenode.heartbeat.recheck-interval=30000 -fs hdfs://boss:9000"]


