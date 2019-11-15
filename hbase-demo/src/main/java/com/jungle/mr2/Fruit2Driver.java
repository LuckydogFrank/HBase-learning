package com.jungle.mr2;

import com.jungle.mr1.FruitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2Driver implements Tool {


    //定义配置信息；一个Configuration
    private Configuration configuration = null;

    public int run(String[] args) throws Exception {

        //1.获取job对象
        Job job = Job.getInstance(configuration);

        //2.设置驱动类路径
        job.setJarByClass(Fruit2Driver.class);

        //3.设置Mapper&Mapper输出KV类型
        TableMapReduceUtil.initTableMapperJob("fruit2",
                new Scan(),   //空参代表全表扫描
                Fruit2Mapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);
        //4.设置Ruducer&输出的表
        TableMapReduceUtil.initTableReducerJob("fruit",Fruit2Reducer.class,job);

        //6.提交任务
        boolean result = job.waitForCompletion(true);

        return result?0:1;
    }

    public void setConf(Configuration conf) {

        configuration =conf;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {




        try {
//            Configuration configuration = new Configuration();
            Configuration configuration = HBaseConfiguration.create();
            int run = ToolRunner.run(configuration, new Fruit2Driver(), args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
