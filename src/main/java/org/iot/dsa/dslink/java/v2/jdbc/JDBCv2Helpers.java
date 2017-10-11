package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSList;

import java.sql.*;
import java.util.Enumeration;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

//import org.apache.commons.dbcp2.BasicDataSource;
//import org.dsa.iot.jdbc.model.JdbcConfig;

class JDBCv2Helpers {
    //////////////////////
    //Global Static Data
    ///////////////////////
    private static DSList cashedDriversNames;
    private static final char[] ALPHA_CHARS;
    private static final Random RANDOM = new Random();

    ////////////////////
    //String Definitions
    ////////////////////
    static final String DB_NAME = "DB Name";
    static final String DB_URL = "URL";
    static final String DB_USER = "User Name";
    static final String DB_PASSWORD = "Password";
    static final String DRIVER = "Driver";
    static final String ADD_DB = "Add DB";
    static final String LAST_SUCCESS = "Last Success Con";
    static final String LAST_FAIL = "Last Fail Con";


    static final String DRIVER_NAME = "Driver Name";
    static final String REGISTERED = "Registered";
    static final String ADD_DRIVER = "Add Driver";

    static final String REMOVE = "Disconnect";

    static final String QUERY = "Query";

    static final String EDIT = "Edit";

    static final String INTERVAL = "Query Interval";
    static final String STREAM_QUERY = "Streaming Query";

    static final String STATUS = "Connection Status";

    ////////////////////////
    //Helper Functions
    ////////////////////////

//    public static BasicDataSource configureDataSource(JdbcConfig config) {
//        BasicDataSource dataSource = new BasicDataSource();
//        // dataSource.setDriverClassName("com.mysql.jdbc.Driver");
//        dataSource.setDriverClassName(config.getDriverName());
//        // dataSource.setUrl(jdbc:mysql://127.0.0.1:3306);
//        dataSource.setUrl(config.getUrl());
//        dataSource.setUsername(config.getUser());
//        dataSource.setPassword(String.valueOf(config.getPassword()));
//        dataSource.setInitialSize(3);
//        dataSource.setMaxIdle(10);
//        dataSource.setMinIdle(1);
//        dataSource.setMaxOpenPreparedStatements(20);
//        dataSource.setTestWhileIdle(false);
//        dataSource.setTestOnBorrow(false);
//        dataSource.setTestOnReturn(false);
//        dataSource.setTimeBetweenEvictionRunsMillis(1);
//        dataSource.setNumTestsPerEvictionRun(50);
//        dataSource.setMinEvictableIdleTimeMillis(1800000);
//        return dataSource;
//    }

    static void registerDriver(String driverClass) throws ClassNotFoundException {
        Class.forName(driverClass);
        cashedDriversNames = null;
    }

    static DSList getRegisteredDrivers() {
        if (cashedDriversNames == null) {
            Enumeration<Driver> drivers = DriverManager.getDrivers();
            cashedDriversNames = new DSList();
            while (drivers.hasMoreElements()) {
                Driver driver = drivers.nextElement();
                // skip MySQL fabric
                if (!driver.getClass().getName().contains("fabric")) {
                    cashedDriversNames.add(driver.getClass().getName());
                }
            }
        }

//        System.out.println("Driver List:");
//        for (DSElement i : cashedDriversNames) {
//            System.out.println(i);
//        }
        return cashedDriversNames.copy();
    }

    static String randomCursorName() {
        char[] buf = new char[8];
        for (int i = 0; i < buf.length; ++i) {
            buf[i] = ALPHA_CHARS[RANDOM.nextInt(ALPHA_CHARS.length)];
        }
        return new String(buf);
    }

    static void cleanClose(ResultSet res, Statement stmt, Connection conn, Logger log) {
        if (res != null) {
            try {
                res.close();
            } catch (SQLException e) {
                log.log(Level.WARNING, "Error closing ResultTable", e);
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.log(Level.WARNING, "Error closing Statement", e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.log(Level.WARNING, "Error closing Connection", e);
            }
        }
    }

    static {
        StringBuilder tmp = new StringBuilder();
        for (char ch = 'a'; ch <= 'z'; ++ch) {
            tmp.append(ch);
        }
        ALPHA_CHARS = tmp.toString().toCharArray();
    }
}
