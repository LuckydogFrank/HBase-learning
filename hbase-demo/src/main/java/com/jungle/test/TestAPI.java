package com.jungle.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * DDL:
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 *
 * DML:
 * 5.插入数据
 * 6.查数据（get）
 * 7.查数据（get）
 * 8.删除数据
 *
 */
public class TestAPI {


    private static Connection connection = null;
    private static Admin admin = null;


    /**
     * 初始化
     */
    static {

        try {
            //1.获取配置文件信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","centosserver1");

            //2.获取管理员对象
            connection = ConnectionFactory.createConnection(configuration);

            //3.创建Admin对象
            admin = connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    /**
     * 1.判断表是否存在
     * @param tableName
     * @return
     */
    public static boolean isTableExist(String tableName) throws IOException {

//        //1.获取配置文件信息
////        HBaseConfiguration configuration = new HBaseConfiguration();
//        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum","centosserver1");
//
//        //2.获取管理员对象
////        HBaseAdmin admin = new HBaseAdmin(configuration);
//        Connection connection = ConnectionFactory.createConnection(configuration);
//        Admin admin = connection.getAdmin();

        //3.判断表是否存在
//        boolean exists = admin.tableExists(tableName);
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        //4.关闭连接
//        admin.close();

        //5.返回结果
        return exists;


    }

    /**
     * 2.创建表
     * @param tableName 表名
     * @param cfs 列簇
     */
    public static void createTable(String tableName,String... cfs) throws IOException {

        //1.判断是否存在列簇信息
        if (cfs.length <= 0) {
            System.out.println("请设置列簇信息！");
            return;
        }

        //2.判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在");
            return;
        }

        //3.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //4.循环添加列族信息
        for (String cf : cfs) {

            //5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //6.添加具体的列簇信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //7.创建表
        admin.createTable(hTableDescriptor);


    }

    /**
     * 3.删除表
     * @param tableName
     */
    public static void dropTable(String tableName) throws IOException {

        //1.判断表是否存在
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在！！！！");
            return;
        }

        //2.使表下线
        admin.disableTable(TableName.valueOf(tableName));

        //3.删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 4.创建命名空间
     * @param ns
     */
    public static void createNameSpace(String ns) {

        //1.创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

        //2.创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e){

            System.out.println(ns + "命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 5.向表插入数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @param value
     */
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //3.给Put对象赋值
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

        //4.插入数据
        table.put(put);

        //5.关闭表连接
        table.close();

    }

    /**
     * 6.获取数据（get）
     * @param tableName
     * @param rowKey
     * @param cn
     * @throws IOException
     */
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //2.1 指定获取的列族
//        get.addFamily(Bytes.toBytes(cf));

        //2.2 指定列簇和列
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));

        //2.3 设置获取数据的版本数
        get.setMaxVersions(5);

        //3.获取数据
        Result result = table.get(get);

        //4.解析result并打印
        for (Cell cell : result.rawCells()) {

            //5.打印数据
            System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                    ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))+
                    ",Value:" + Bytes.toString(CellUtil.cloneValue(cell))

                    );
        }

        //6.关闭表连接
        table.close();
    }


    /**
     * 7.获取数据（scan）
     * @param tableName
     * @throws IOException
     */
    public static void scanTable(String tableName) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.构建Scan对象
//        Scan scan = new Scan();
        Scan scan = new Scan(Bytes.toBytes("1001"),Bytes.toBytes("1020"));

        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析resultScanner
        for (Result result : resultScanner) {

            //5.解析result并打印
            for (Cell cell : result.rawCells()) {

                //6.打印数据
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",CF:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))+
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell))

                );

            }
        }
        //7.关闭表连接
        table.close();
    }

    /**
     * 8.删除数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     */
    public static void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //2.1 设置删除的列
//        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),1573368881383L);

        //2.2 删除指定列族
        delete.addFamily(Bytes.toBytes(cf));

        //3.执行删除操作
        table.delete(delete);

        //4.关闭连接
        table.close();
    }

    /**
     * 关闭资源
     */
    public static void close(){
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws IOException {

        //1.测试表是否存在
//        System.out.println(isTableExist("stu5"));

        //2.创建表测试
//        createTable("stu5","info1","info2");
        createTable("test:st","info1","info2");

        //3.删除表测试
//        dropTable("stu5");

        //4.创建命名空间测试
//        createNameSpace("test");

        //5.创建数据测试
//        putData("student","1004","info","sex","nan");

        //6.获取单行数据
//        getData("student","1001","info","sex");


        //7.测试扫描数据
//        scanTable("student");

        //8.测试删除
//        deleteData("student","1004","","");
//        System.out.println(isTableExist("stu5"));


        close();
    }
}
