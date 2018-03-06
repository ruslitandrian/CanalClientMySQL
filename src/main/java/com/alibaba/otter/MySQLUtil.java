package com.alibaba.otter;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class MySQLUtil {

    private static String ACCOUNT = "XXXX";
    private static String PWD = "XXXX";
    private static String ADDR = "XXX.XXX.XXXX.XXX";

    private static Connection con = null;   //連接object
    private static Statement stat = null;    //執行,傳入之sql為完整字串


    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //註冊driver
            String url="jdbc:mysql://"+ ADDR + ":3306/?verifyServerCertificate=false&autoReconnect=true&useSSL=true&useUnicode=true&characterEncoding=utf8";
            con = DriverManager.getConnection(url, ACCOUNT,PWD);
            //取得connection
        }
        catch(ClassNotFoundException e)
        {
            System.out.println("DriverClassNotFound :"+e.toString());
        }//有可能會產生sqlexception
        catch(SQLException x) {
            System.out.println("Exception :"+x.toString());
        }
    }

    //建立table的方式
    //可以看看Statement的使用方式
    public static void QuerySQL(String sqlStatement)
    {
        try
        {
            stat = con.createStatement();
            stat.executeUpdate(sqlStatement);
        }
        catch(SQLException e)
        {
            System.out.println("CreateDB Exception :" + e.toString());
        }
        finally
        {
            Close();
        }
    }

    //完整使用完資料庫後,記得要關閉所有Object
    //否則在等待Timeout時,可能會有Connection poor的狀況
    private static void Close()
    {
        try
        {
            if(stat!=null)
            {
                stat.close();
                stat = null;
            }
        }
        catch(SQLException e)
        {
            System.out.println("Close Exception :" + e.toString());
        }
    }


    public static void Insert(CanalEntry.Entry entry, List<CanalEntry.Column> columns)
    {
        String dbName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        StringBuilder sql = new StringBuilder();
        StringBuilder sqlColumn = new StringBuilder();
        StringBuilder sqlValue = new StringBuilder();
        sql.append("INSERT INTO `"+ dbName + "`.`"+ tableName + "` (");
        int index = 0;
        for (CanalEntry.Column column : columns) {
            if(index > 0){
                sqlColumn.append(',');
                sqlValue.append(',');
            }
            sqlColumn.append(column.getName());
            sqlValue.append("'" + column.getValue() + "'");
            index++;
        }
        sql.append(sqlColumn.toString() + " ) VALUES(" + sqlValue.toString() +");");
        System.out.println(sql.toString());
        if(columns.size()>0) {
            MySQLUtil.QuerySQL(sql.toString());
        }
    }

    public static void Delete(CanalEntry.Entry entry, List<CanalEntry.Column> columns)
    {
        String dbName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        StringBuilder sql = new StringBuilder();
        StringBuilder sqlColumn = new StringBuilder();
        sql.append("DELETE FROM  `"+ dbName + "`.`"+ tableName + "`  ");
        int index = 0;
        for (CanalEntry.Column column : columns) {
            if (index > 0) {
                sqlColumn.append(" AND ");
            }
            sqlColumn.append(column.getName() + " = '" + column.getValue() + "'");
            index++;
        }

        sql.append(" WHERE " + sqlColumn.toString());
        System.out.println(sql.toString());
        if(columns.size()>0) {
            MySQLUtil.QuerySQL(sql.toString());
        }
    }

    public static void Update(CanalEntry.Entry entry, List<CanalEntry.Column> columns){
        String dbName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        StringBuilder sql = new StringBuilder();
        StringBuilder sqlColumn = new StringBuilder();
        StringBuilder sqlKey = new StringBuilder();
        int index = 0;
        for (CanalEntry.Column column : columns) {

            if(column.getIsKey())
            {
                sqlKey.append(column.getName() + " = '" + column.getValue() + "'");
            }
            else
            {
                sqlColumn.append(column.getName() + " = '" + column.getValue() + "'");
            }

            if(index > 1){
                sqlColumn.append(',');
            }
            index++;
        }

        sql.append(" UPDATE   `"+ dbName + "`.`"+ tableName + "`  ");
        if(columns.size()>0) {
            sql.append(" SET " + sqlColumn.toString());
            sql.append(" WHERE " + sqlKey.toString());
            System.out.println(sql.toString());
            MySQLUtil.QuerySQL(sql.toString());
        }else{
            System.out.println("Statement Empty!");
        }
    }
}