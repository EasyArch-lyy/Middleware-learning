package com.example.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Test{

    public static void main(String[] args){
    	//配置类，通过连接zookeeper连接hbase，后面跟主机名，不要跟ip地址，自己配置windows映射关系
        Configuration conf=new Configuration();
        conf.set("hbase.zookeeper.quorum","192.168.119.128");
        try {
        	//连接hbase
            Connection con=ConnectionFactory.createConnection(conf);
            //获取t01表
            Table table=con.getTable(TableName.valueOf("t01"));
            //向t01表中添加一条数据
            Put put=new Put(Bytes.toBytes("001"));
            put.addColumn(Bytes.toBytes("cf01"),Bytes.toBytes("age"),Bytes.toBytes("100"));
            table.put(put);
            //查询t01表中所有行数据
            Scan scan=new Scan();
            ResultScanner result=table.getScanner(scan);//得到的是数据集
            Result str=null;
            while((str=result.next())!=null){
                List<Cell> cells = str.listCells();//将每条数据存到Cell对象里
                for (Cell c:cells) {
                    System.out.println(new String(CellUtil.cloneFamily(c))+new String(CellUtil.cloneQualifier(c))+new String(CellUtil.cloneValue(c)));
                }
            }
            //得到一行数据或一条数据
            Get get=new Get("001".getBytes());
            get.addColumn("cf01".getBytes(),"name".getBytes());
            Result result1=table.get(get);//
            List<Cell> cells = result1.listCells();
            for (Cell c:cells) {
                System.out.println(new String(CellUtil.cloneFamily(c))+new String(CellUtil.cloneQualifier(c))+new String(CellUtil.cloneValue(c)));
            }
            //插入1000条数据
            List<Put> list=new ArrayList<Put>();
            for (int i = 0; i < 1000; i++) {
                Put put1=new Put(Bytes.toBytes("00"+i));
                put1.addColumn("cf01".getBytes(),"name".getBytes(),("name_"+i).getBytes());
                list.add(put1);
            }
            table.put(list);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

