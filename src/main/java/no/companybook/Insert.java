package no.companybook;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

class Insert {
    private static void configure(Configuration config, String quorum, String port, String parent) {
        config.set("hbase.zookeeper.property.clientPort", port);
        config.set("hbase.zookeeper.quorum", quorum);
        config.set("zookeeper.znode.parent", parent);
    }

    public static void main (String[] args) throws IOException, URISyntaxException {
        String quorum = args[0];
        String port = args[1];
        String parent = args[2];
        byte[] familyName = Bytes.toBytes(args[3]);
        String column = args[4];
        byte[] columnName = Bytes.toBytes(column);
        String tableName = args[5];
        Configuration config = new Configuration();
        String inputFile = args[6];
        config.set("fs.hdfs.impl", 
            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        config.set("fs.file.impl",
            org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        configure(config, quorum, port, parent);
        Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf(tableName));
        InputStream is = new FileInputStream(new File(inputFile));
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        String line;
        int i = 0;
        try {
            while ((line = reader.readLine()) != null) {
                String[] splits = line.split("\t", 2);
                String key = splits[0];
                String value = splits[1];
                Put put = new Put(Bytes.toBytes(key));
                put.addColumn(familyName, columnName, Bytes.toBytes(value));
                table.put(put);
                if (++i % 100 == 0) {
                    System.out.println("Put: <" + put.toString() + ">");
                }
            }
        } finally {
            reader.close();
            table.close();
            connection.close();
        }
    }
}

