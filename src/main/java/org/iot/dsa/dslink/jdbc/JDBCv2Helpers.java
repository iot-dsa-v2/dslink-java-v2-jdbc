package org.iot.dsa.dslink.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.iot.dsa.node.DSNode;

/**
 * Class for storing helper methods and variables.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
public class JDBCv2Helpers {

	public static final String DB_NAME = "DB Name";
	public static final String DB_URL = "URL";
	public static final String DB_USER = "User Name";
	public static final String DB_PASSWORD = "Password";
	public static final String DRIVER = "Driver";
	public static final String ADD_DB = "Connect";
	public static final String EXT_ACCESS = "Allow Access";
	public static final String CREATE_DB = "Create H2";
	public static final String DRIVER_NAME = "Driver Name";
	public static final String REGISTERED = "Registered";
	public static final String ADD_DRIVER = "Add Driver";
	public static final String QUERY = "Query";
	public static final String UPDATE = "Update";
	public static final String MAKE_NODES = "Nodes";
	public static final String SHOW_TABLES = "Show Tables";
	public static final String SHOW_TABLE = "Show Table";
	public static final String EDIT = "Edit";
	public static final String INTERVAL = "Query Interval";
	public static final String STREAM_QUERY = "Streaming Query";

    public static void cleanClose(ResultSet res, Statement stmt, Connection conn, DSNode node) {
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
}
