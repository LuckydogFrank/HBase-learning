
package com.jungle.utils;

import com.jungle.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表（三张表）
 */
public class HBaseUtil {

    /**
     * 1.创建命名空间
     * @param nameSpace
     * @throws IOException
     */
    public static void createNameSpace(String nameSpace) throws IOException {

        //1.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2.获取Admin对象
        Admin admin = connection.getAdmin();

        //3.构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();


        //4.创建命名空间
        admin.createNamespace(namespaceDescriptor);

        //5.关闭资源
        admin.close();
        connection.close();


    }

    /**
     * 2.判断表是否存在
     * @param tableName
     * @return
     */
    private static boolean isTableExist(String tableName) throws IOException {

        //1.获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2.获取Admin对象
        Admin admin = connection.getAdmin();

        //3.判断是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        //4.关闭资源
        admin.close();
        connection.close();

        //返回结果
        return exists;


    }

    /**
     * 3.创建表
     * @param tableName
     * @param versions
     * @param cfs
     */
    public static void createTable(String tableName,int versions,String... cfs) throws IOException {

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
            //3.获取Connection对象
            Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

            //4.获取Admin对象
            Admin admin = connection.getAdmin();

            //5.创建表描述器
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            //6.循环添加列族信息
            for (String cf : cfs) {

                //7.创建列族描述器
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

                hColumnDescriptor.setMaxVersions(versions);
                //8.添加具体的列簇信息
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            //9.创建表
            admin.createTable(hTableDescriptor);

            //关闭资源
            admin.close();
            connection.close();

        }

    }

