package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.*;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.security.DSPasswordAes;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import com.mchange.v2.c3p0.*;

public class DBConnectionNode extends DSNode {

    private final DSInfo db_name = getInfo(JDBCv2Helpers.DB_NAME);
    private final DSInfo db_url = getInfo(JDBCv2Helpers.DB_URL);
    private final DSInfo usr_name = getInfo(JDBCv2Helpers.DB_USER);
    private final DSInfo password = getInfo(JDBCv2Helpers.DB_PASSWORD);
    private final DSInfo driver = getInfo(JDBCv2Helpers.DRIVER);
    private final DSInfo conn_status = getInfo(JDBCv2Helpers.STATUS);
    //private Connection conn = null;
    private ComboPooledDataSource pool_data_source = null;

    public DBConnectionNode () {

    }

    DBConnectionNode (DSMap params) {
        put(db_name, params.get(JDBCv2Helpers.DB_NAME));
        put(db_url, params.get(JDBCv2Helpers.DB_URL));
        put(usr_name, params.get(JDBCv2Helpers.DB_USER));
        put(driver, params.get(JDBCv2Helpers.DRIVER));
        put(password, DSPasswordAes.valueOf(params.get(JDBCv2Helpers.DB_PASSWORD).toString()));
    }

    //////////////////////////////////////
    //Database Actions
    //////////////////////////////////////

    private DSAction makeQueryAction() {
        return new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                ((DBConnectionNode) info.getParent()).runQuery();
                return null;
            }
        };
    }

    private void runQuery() {
        //TODO: add database query code here
    }

    private DSAction makeRemoveDatabaseAction() {
        return new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                ((DBConnectionNode) info.getParent()).removeDatabase();
                return null;
            }
        };
    }

    private void removeDatabase() {
        if (pool_data_source != null) {
            pool_data_source.close();
        }
        getParent().remove(getInfo());
    }

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        //Default Values
        declareDefault(JDBCv2Helpers.DB_NAME, DSString.valueOf("No Name"));
        declareDefault(JDBCv2Helpers.DB_USER, DSString.valueOf("No Name"));
        declareDefault(JDBCv2Helpers.DB_URL, DSString.valueOf("No URL"));
        declareDefault(JDBCv2Helpers.DRIVER, DSString.valueOf("No Driver"));
        declareDefault(JDBCv2Helpers.DB_PASSWORD, DSPasswordAes.valueOf("No Pass")).setHidden(true);
        declareDefault(JDBCv2Helpers.STATUS, DSString.valueOf(ConnStates.Unknown));
        //Default Actions
        declareDefault(JDBCv2Helpers.QUERY, makeQueryAction());
        declareDefault(JDBCv2Helpers.REMOVE, makeRemoveDatabaseAction());
    }

    @Override
    protected void onStable() {
        try {
            String url = db_url.getValue().toString();
            String name = usr_name.getValue().toString();
            String pass = ((DSPasswordAes) password.getValue()).decode();
            String drvr = driver.getValue().toString();
            //Debug message
            //info(url + ", " + name + ", " + pass);
            pool_data_source = new ComboPooledDataSource();
            pool_data_source.setDriverClass(drvr); //loads the jdbc driver
            pool_data_source.setJdbcUrl(url);
            pool_data_source.setUser(name);
            pool_data_source.setPassword(pass);

            //Alternative, uses standard JDBC drivers
            /*
            DataSource ds_unpooled = DataSources.unpooledDataSource(url, name, pass);
            DataSource ds_pooled = DataSources.pooledDataSource( ds_unpooled );
            */

            //conn = DriverManager.getConnection(url, name, pass);
            put(conn_status, DSString.valueOf(ConnStates.Connected));
        } catch (PropertyVetoException e) {
            put(conn_status, DSString.valueOf(ConnStates.Failed));
            warn("Failed to connect to Database: " + db_name.getValue());
            warn(e);
        }
    }

    public enum ConnStates {
        Connected,
        Failed,
        Unknown
    }
}
