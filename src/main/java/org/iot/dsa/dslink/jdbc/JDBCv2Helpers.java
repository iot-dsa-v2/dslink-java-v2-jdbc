package org.iot.dsa.dslink.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Random;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSNode;

/**
 * Class for storing helper methods and variables.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
class JDBCv2Helpers {

    private static final char[] ALPHA_CHARS;
    private static final Random RANDOM = new Random();

    static final String DB_NAME = "DB Name";
    static final String DB_URL = "URL";
    static final String DB_USER = "User Name";
    static final String DB_PASSWORD = "Password";
    static final String DRIVER = "Driver";
    static final String ADD_DB = "Connect";
    static final String LAST_SUCCESS = "Last Success Con";
    static final String LAST_FAIL = "Last Fail Con";
    static final String EXT_ACCESS = "Allow Access";
    static final String CREATE_DB = "Create H2";
    static final String DRIVER_NAME = "Driver Name";
    static final String REGISTERED = "Registered";
    static final String ADD_DRIVER = "Add Driver";
    static final String REMOVE = "Disconnect";
    static final String QUERY = "Query";
    static final String UPDATE = "Update";
    static final String MAKE_NODES = "Nodes";
    static final String SHOW_TABLES = "Show Tables";
    static final String SHOW_TABLE = "Show Table";
    static final String EDIT = "Edit";
    static final String INTERVAL = "Query Interval";
    static final String STREAM_QUERY = "Streaming Query";
    static final String STATUS = "Connection Status";

    private static DSList cashedDriversNames;

    static void cleanClose(ResultSet res, Statement stmt, Connection conn, DSNode node) {
        if (res != null) {
            try {
                res.close();
            } catch (SQLException e) {
                node.warn(node.getPath(), e);
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                node.warn(node.getPath(), e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                node.warn(node.getPath(), e);
            }
        }
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

        return cashedDriversNames.copy();
    }

    static void registerDriver(String driverClass) throws ClassNotFoundException {
        Class.forName(driverClass);
        cashedDriversNames = null;
    }

    static {
        StringBuilder tmp = new StringBuilder();
        for (char ch = 'a'; ch <= 'z'; ++ch) {
            tmp.append(ch);
        }
        ALPHA_CHARS = tmp.toString().toCharArray();
    }
}
