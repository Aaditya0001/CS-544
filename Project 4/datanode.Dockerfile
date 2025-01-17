# Use p4-hdfs as the base image
FROM p4-hdfs

# Run the DataNode with specified configurations
CMD ["sh", "-c", "hdfs datanode -D dfs.datanode.data.dir=/var/datanode -fs hdfs://boss:9000"]

