package no.companybook;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class Extract {
    private static void configure(Configuration config, String quorum, String port, String parent) {
        config.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        config.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        config.set("hbase.zookeeper.property.clientPort", port);
        config.set("hbase.zookeeper.quorum", quorum);
        config.set("zookeeper.znode.parent", parent);
    }


    public static void main (String[] args) throws IOException, URISyntaxException {
        String quorum = args[0];
        String port = args[1];
        String parent = args[2];
        byte[] familyName = Bytes.toBytes(args[3]);
        List<String> columnNames = Arrays.stream(args[4].split(","))
                .map(cn -> cn.replaceAll("\\s+", ""))
                .collect(Collectors.toList());
        String tableName = args[5];
        String country = args[6].toUpperCase();
        String outputFileName = args[7];
        Configuration config = new Configuration();
        configure(config, quorum, port, parent);
        Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf(tableName));
        OutputStream os = new FileOutputStream(new File(outputFileName));
        Writer writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        Scan scan = new Scan();
        scan.setMaxVersions(1);
//        scan.setTimeRange(1467114931000L, 1474444127000L);
//        scan.setTimeRange(1450000000000L, 1500000000000L);
        scan.setStartRow(Bytes.toBytes(country + "0"));
        scan.setStopRow(Bytes.toBytes(country + "Z"));
        for (String columnName: columnNames) {
            scan.addColumn(familyName, Bytes.toBytes(columnName));
        }
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                List<String> columnValues = new ArrayList<>();
                for (String columnName: columnNames) {
                    columnValues.add(Bytes.toString(result.getValue(familyName, Bytes.toBytes(columnName))));
                }
                System.out.println("Row: " + Bytes.toString(result.getRow()));
                writer.write(Bytes.toString(result.getRow()) + "\t" + String.join("\t", columnValues) + "\n");
            }
        } finally {
            writer.close();
            scanner.close();
            table.close();
        }
    }
}
