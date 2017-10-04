package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.DSList;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

//import org.apache.commons.dbcp2.BasicDataSource;
//import org.dsa.iot.jdbc.model.JdbcConfig;

class JDBCv2Helpers {
    //////////////////////
    //Global Static Data
    ///////////////////////
    private static String[] cashedDriversName;

    ////////////////////
    //String Definitions
    ////////////////////
    static final String DB_NAME = "DB Name";
    static final String DB_URL = "URL";
    static final String DB_USER = "User Name";
    static final String DB_PASSWORD = "Password";
    static final String DRIVER = "Driver";
    static final String ADD_DB = "Add DB";

    static final String REMOVE = "Disconnect";

    static final String QUERY = "Query";

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

    public static DSList getRegisteredDrivers() {
        if (cashedDriversName == null) {
            Enumeration<Driver> drivers = DriverManager.getDrivers();
            Set<String> set = new HashSet<>();
            while (drivers.hasMoreElements()) {
                Driver driver = drivers.nextElement();
                // skip MySQL fabric
                if (!driver.getClass().getName().contains("fabric")) {
                    set.add(driver.getClass().getName());
                }
            }
            cashedDriversName = set.toArray(new String[set.size()]);
        }
        DSList lst = new DSList();
        System.out.println("Driver List:");
        for (String str : cashedDriversName) {
            lst.add(str);
            System.out.println(str);
        }
        return lst;
    }
}
