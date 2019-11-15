package com.jungle.test;

import com.jungle.constants.Constants;
import com.jungle.dao.HBaseDao;
import com.jungle.utils.HBaseUtil;

import java.io.IOException;

public class TestWeiBo {

    public static void init() {


        try {
            //创建命名空间
            HBaseUtil.createNameSpace(Constants.NAMESPACE);

            //创建微博内容表
            HBaseUtil.createTable(Constants.CONTENT_TABLE,Constants.CONTENT_TABLE_VERSIONS,
                    Constants.CONTENT_TABLE_CF);
            System.out.println("内容表已好");

            //创建用户关系表
            HBaseUtil.createTable(Constants.RELATION_TABLE,Constants.RELATION_TABLE_VERSIONS,Constants.RELATION_TABLE_CF1, Constants.RELATION_TABLE_CF2);
            System.out.println("用户关系表已好");

            //创建收件箱表
            HBaseUtil.createTable(Constants.INBOX_TABLE,Constants.INBOX_TABLE_VERSIONS,
                    Constants.INBOX_TABLE_CF);
            System.out.println("收件箱表已好");

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void main(String[] args) throws IOException, InterruptedException {

        //初始化
        init();

        //1001发布微博
        HBaseDao.publishWeiBo("1001","赶紧下课吧！！！");

        //1002关注1001和1003
        HBaseDao.addAttends("1002","1001","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");

        System.out.println("~~~~~~~~~~~~~~111~~~~~~~~~~~~~~");

        //1003发布3条微博，同时1001发布2条微博
        HBaseDao.publishWeiBo("1003","谁说得赶紧下课！！！！");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1001","我没说话！！！！");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1003","那谁说的！！！！");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1001","反正飞机下线了！！！！");
        Thread.sleep(10);

        HBaseDao.publishWeiBo("1001","你们爱咋咋地！！！！");
        Thread.sleep(10);

        //获取1002初始化页面
        HBaseDao.getInit("1002");

        System.out.println("~~~~~~~~~~~~~~222~~~~~~~~~~~~~~");


        //1002取关1003
        HBaseDao.deleteAttends("1002","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("~~~~~~~~~~~~~~333~~~~~~~~~~~~~~");

        //1002再次关注1003
        HBaseDao.addAttends("1002","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("~~~~~~~~~~~~~~444~~~~~~~~~~~~~~");

        //获取1001微博详情
        HBaseDao.getWeiBo("1001");
    }
}
