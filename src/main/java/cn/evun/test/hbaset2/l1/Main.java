package cn.evun.test.hbaset2.l1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

public class Main {


    private Configuration configuration;
    private Connection connection;
    private byte[] row1 = Bytes.toBytes("row1");
    private byte[] row2 = Bytes.toBytes("row2");
    private byte[] cf1 = Bytes.toBytes("cf1");
    private byte[] cf2 = Bytes.toBytes("cf2");
    private byte[] qf1 = Bytes.toBytes("qf1");
    private byte[] qf2 = Bytes.toBytes("qf2");



    @Before
    public void before() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "10.190.35.130");// zookeeper地址
        configuration.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
        connection = ConnectionFactory.createConnection(configuration);
    }





    private TableName tableName = TableName.valueOf("testTable");

    @Test
    public void testCreateTable() throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor cfDesc = new HColumnDescriptor("base");
        tableDesc.addFamily(cfDesc);
        admin.createTable(tableDesc);
        System.out.println("table available :" + admin.isTableAvailable(tableName));
    }

    @Test
    public void testModTable() throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("address"));
        admin.disableTable(tableName);
        admin.modifyTable(tableName,tableDescriptor);
        admin.enableTable(tableName);

    }
    @Test
    public void testJQ() throws IOException {
        Admin admin = connection.getAdmin();
        ClusterStatus clusterStatus = admin.getClusterStatus();
        System.out.println(clusterStatus);
    }



















    @After
    public void after() throws IOException {
        connection.close();
    }
    @Test
    public void test1() throws IOException {
        HTable test = (HTable) connection.getTable(TableName.valueOf("test"));
//        test.setAutoFlush(false,false);
        System.out.println(test.isAutoFlush());
//        test.setWriteBufferSize(1);
        System.out.println(test.getWriteBufferSize());
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("x"),Bytes.toBytes("vh"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("y"),Bytes.toBytes(123));
        test.put(put);
        put = new Put(Bytes.toBytes("row2"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("z"),Bytes.toBytes("vx"));
        test.put(put);
        test.close();
    }
    @Test
    public void testPuts() throws IOException {
        HTable test = (HTable) connection.getTable(TableName.valueOf("test1"));
        test.setAutoFlush(false,true);
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("r4"));
        put1.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("cq1"),Bytes.toBytes("v1"));
        puts.add(put1);
        Put put2 = new Put(Bytes.toBytes("r2"));
        put2.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("cq1"),Bytes.toBytes("v2"));
        puts.add(put2);
        Put put3 = new Put(Bytes.toBytes("r5"));
        put3.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("cq1"),Bytes.toBytes("v3"));
        puts.add(put3);
        Put put4 = new Put(Bytes.toBytes("r6"));
        puts.add(put4);
        try {
            test.put(puts);
        }catch (Exception e){
            System.out.println(e);
            test.flushCommits();
        }
        test.close();
    }
    @Test
    public void testCheckAndPut() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test1"));
        Put put = new Put(Bytes.toBytes("rowc1"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("cq"),Bytes.toBytes("spec"));
        boolean success = table.checkAndPut(Bytes.toBytes("rowc1"),Bytes.toBytes("cf"),Bytes.toBytes("cq"),null,put);
        System.out.println(success);
        table.close();
    }

    @Test
    public void testGet() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test1"));
        Get get = new Get(Bytes.toBytes("rowc1"));
        Result result = table.get(get);
        /*byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("cq"));
        System.out.println(Bytes.toString(value));*/
//        NavigableMap<byte[], byte[]> cf = result.getFamilyMap(Bytes.toBytes("cf"));
//        cf.forEach((bs1,bs2)->{
//            System.out.println(Bytes.toString(bs1)+ " -> "+Bytes.toString(bs2));
//        });
        List<Cell> cells = result.listCells();
        cells.forEach(c ->{
            System.out.println(Bytes.toString(c.getRowArray())+ "," + Bytes.toString(c.getFamilyArray())
                    + "," + Bytes.toString(c.getQualifierArray())+","+c.getTimestamp()+"," + Bytes.toString(c.getValueArray()));
        });
    }
    @Test
    public void testGets() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test1"));
        List<Get> gets = new ArrayList<>(10);
        Get get1 = new Get(row1);
        get1.addColumn(cf1,qf1);
        gets.add(get1);
        System.out.println(table.exists(get1));
        Get get2 = new Get(row2);
        get2.addFamily(cf1);
        gets.add(get2);
        Get get3 = new Get(row2);
        get3.addColumn(cf2,qf1);
        gets.add(get3);
        Result[] results = table.get(gets);
        for (Result result:
             results) {
            if(result == null || result.getMap()==null){
                continue;
            }
            System.out.println(Bytes.toString(result.getRow()));
            result.getMap().forEach((bytes,map)->{
                System.out.println(Bytes.toString(bytes));
                map.forEach((cq,map1)->{
                    System.out.println(Bytes.toString(cq));
                    map1.forEach((t,v)->{
                        System.out.println(t + "," + Bytes.toString(v));
                    });
                });
            });
        }
    }
    @Test
    public void testSpecGet() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test1"));
    }
    @Test
    public void testDel() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test1"));
        Delete delelte = new Delete(row1);
        delelte.addColumn(cf1,qf1).addColumn(cf2,qf2);
        table.delete(delelte);
        table.close();
    }
    @Test
    public void testScanner() throws IOException, InterruptedException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test1"));
        Scan scan = new Scan();
        scan.setCaching(10);
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result ->{
            System.out.println(result);
        });
        scanner.close();

        System.out.println("=========================================");
        Scan scan1 = new Scan();
        scan1.addFamily(cf2);
        ResultScanner scanner1 = table.getScanner(scan1);
        scanner1.forEach(result1 ->{
            System.out.println(result1);
        });
        scanner1.close();

        System.out.println("==========================================");
        Scan scan2 = new Scan();
        scan2.addColumn(cf1,qf2);
        ResultScanner scanner2 = table.getScanner(scan2);
        scanner2.forEach(result2 -> {
            System.out.println(result2);
        });
        scanner2.close();

        Scan scan3 = new Scan();
        ResultScanner scanner3 = table.getScanner(scan3);
        long aLong = configuration.getLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1);
        System.out.println(aLong);
        Thread.sleep(aLong + 1000);
        scanner3.forEach(result3->{
            System.out.println(result3);
        });
        scanner3.close();
    }

    @Test
    public void testFilter() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test"));
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.LESS,new BinaryComparator(Bytes.toBytes("r3"))));
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });
        scanner.close();
        System.out.println("======================================");
        Scan scan1 = new Scan();
        scan1.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("row*")));
        ResultScanner scanner1 = table.getScanner(scan1);
        scanner1.forEach(result1->{
            System.out.println(result1);
        });
        scanner1.close();
        System.out.println("======================================");
        Scan scan2 = new Scan();
        scan2.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("1")));
        ResultScanner scanner2 = table.getScanner(scan2);
        scanner2.forEach(result2->{
            System.out.println(result2);
        });
        scanner2.close();
    }

    @Test
    public void testFamilyFilter() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test3"));
        Scan scan = new Scan();
        scan.setFilter(new FamilyFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("base"))));
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });
        scanner.close();
    }
    @Test
    public void testColumnFilter() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test3"));
        Scan scan = new Scan();
        scan.setFilter(new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("age"))));
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });
        scanner.close();
    }
    @Test
    public void testValueFilter() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test3"));
        Scan scan = new Scan();
        ValueFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("guang"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });
        scanner.close();

        System.out.println("=================");

        Get get = new Get(Bytes.toBytes("row2"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println(result);

        table.close();
    }
    @Test
    public void testSingleColumnValueFilter() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test3"));
        Scan scan = new Scan();
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("address"), Bytes.toBytes("city"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("hangzhou"));
        singleColumnValueFilter.setFilterIfMissing(true);
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });
        scanner.close();

        System.out.println("========================");

        Scan scan1 = new Scan();
        SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter(Bytes.toBytes("address")
                , Bytes.toBytes("city")
                , CompareFilter.CompareOp.EQUAL
                , new SubstringComparator("guang"));
        singleColumnValueFilter1.setFilterIfMissing(true);
        scan1.setFilter(singleColumnValueFilter1);
        ResultScanner scanner1 = table.getScanner(scan1);
        scanner1.forEach(result1 -> {
            System.out.println(result1);
        });
        scanner1.close();
        System.out.println("========================");
        Get get = new Get(Bytes.toBytes("row2"));
        get.setFilter(singleColumnValueFilter1);
        Result result = table.get(get);
        System.out.println(result);
    }
    @Test
    public void testExSingle() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("test3"));
        Scan scan = new Scan();
        SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes("address"),
                Bytes.toBytes("city"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("guangzhou"));
        singleColumnValueExcludeFilter.setFilterIfMissing(true);
        scan.setFilter(singleColumnValueExcludeFilter);
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });
    }
    @Test
    public void testIncr() throws IOException {
        HTable table = (HTable) connection.getTable(TableName.valueOf("counters"));
//        long l = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
//        System.out.println(l);
        Increment increment = new Increment(Bytes.toBytes("20110101"));
        increment.addColumn( Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
        increment.addColumn( Bytes.toBytes("weekly"), Bytes.toBytes("hits"), 1);
        table.increment(increment);
        table.close();
    }
}
