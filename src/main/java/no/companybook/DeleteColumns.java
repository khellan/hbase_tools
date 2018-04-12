package no.companybook;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class DeleteColumns {
    private static void configure(Configuration config, String quorum, String port, String parent) {
        config.set("hbase.zookeeper.property.clientPort", port);
        config.set("hbase.zookeeper.quorum", quorum);
        config.set("zookeeper.znode.parent", parent);
    }

    public static void main (String[] args) throws IOException {
        String quorum = args[0];
        String port = args[1];
        String parent = args[2];
        byte[] familyName = Bytes.toBytes(args[3]);
        if (familyName.length < 1) {
            System.out.println("Empty column family is not allowed. Please specify column family");
            System.exit(1);
        }
        String rawColumns = args[4];
        if (rawColumns.length() < 1) {
            System.out.println("Deleting whole column families is not allowed. You need to specify columns");
            System.exit(2);
        }
        List<String> columnNames = Arrays.stream(rawColumns.split(","))
                .map(cn -> cn.replaceAll("\\s+", ""))
                .collect(Collectors.toList());
        String tableName = args[5];
        String country = args[6].toUpperCase();

        Configuration config = new Configuration();
        configure(config, quorum, port, parent);
        Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf(tableName));
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
                Delete delete = new Delete(result.getRow());
                for (String columnName: columnNames) {
                    delete.addColumn(familyName, Bytes.toBytes(columnName));
                }
                System.out.println("Row: " + Bytes.toString(result.getRow()));
                table.delete(delete);
            }
        } finally {
            scanner.close();
            table.close();
        }
    }
}
