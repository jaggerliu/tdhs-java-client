/*
 * Copyright(C) 2011-2012 Alibaba Group Holding Limited
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *  Authors:
 *    wentong <wentong@taobao.com>
 */

package com.taobao.tdhs.jdbc.test;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 12-3-20 下午3:47
 */
public class SelectJDBCTest extends TestBase {

    public static void executeSelect(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = null;
        try {
            boolean r = statement.execute("select id,user_id,traveldate,fee,days from travelrecord where id>0 and id< 10");
            Assert.assertTrue(r);
            resultSet = statement.getResultSet();
            int size = 0;
            while (resultSet.next()) {
            	System.out.println(resultSet.getLong(1));
            	System.out.println(resultSet.getString(2));
            	System.out.println(resultSet.getDate(3));
            	System.out.println(resultSet.getString(4));
                size++;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            connection.close();
        }
    }

    public static void executeCount(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = null;
        try {
            boolean r = statement.execute("select count(*) as c from travelrecord where id>0");
            Assert.assertTrue(r);
            resultSet = statement.getResultSet();
            int size = 0;
            while (resultSet.next()) {
            	System.out.println(resultSet.getInt(1));
                size++;
            }
            Assert.assertEquals(1, size);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            connection.close();
        }
    }


    @Test
    public void testMySQLGetData() throws ClassNotFoundException, SQLException {
        executeSelect(getMySQLConnection());
    }

    @Test
    public void testTDHSGetData() throws ClassNotFoundException, SQLException {
        executeSelect(getTDHSConnection());
    }

    @Test
    public void testMySQLGetCount() throws ClassNotFoundException, SQLException {
        executeCount(getMySQLConnection());
    }

    @Test
    public void testTDHSGetCount() throws ClassNotFoundException, SQLException {
        executeCount(getTDHSConnection());
    }
    
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
    	TestBase._init();
   	 //new SelectJDBCTest().executeSelect(getMySQLConnection());
    	 new SelectJDBCTest().executeSelect(getTDHSConnection());
    	 new SelectJDBCTest().executeCount(getTDHSConnection());
    }
}
