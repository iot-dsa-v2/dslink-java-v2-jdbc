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
public interface JDBCObject {

    String DB_NAME = "DB Name";
    String DB_URL = "URL";
    String DB_USER = "User Name";
    String DB_PASSWORD = "Password";
    String DRIVER = "Driver";
    String NAME = "Name";
    String SETTINGS = "Settings";
    String STATEMENT = "Statement";

    /**
     * Most parameters are options.
     *
     * @param res  Optional.
     * @param stmt Optional.
     * @param conn Optional.
     * @param node Required for logging.
     */
    public default void cleanClose(ResultSet res, Statement stmt, Connection conn, DSNode node) {
        if (res != null) {
            try {
                res.close();
            } catch (SQLException e) {
                if (node != null) {
                    node.debug(node.getPath(), e);
                }
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                if (node != null) {
                    node.debug(node.getPath(), e);
                }
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                if (node != null) {
                    node.debug(node.getPath(), e);
                }
            }
        }
    }
}
