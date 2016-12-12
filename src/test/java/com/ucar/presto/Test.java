package com.ucar.presto;


import java.sql.*;

/**
 * @author zhaoshb
 * @since 1.0
 */
public class Test {


    @org.junit.Test
    public void test() throws ClassNotFoundException, SQLException {
        Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        Connection connection = DriverManager.getConnection("jdbc:presto://10.104.102.184:8888" , "fcarbigdata","");
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select * from fcar_bi_common.dbo.marketing_uc");
        while(rs.next()){
        }
    }
}
