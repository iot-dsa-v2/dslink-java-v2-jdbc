package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.dslink.DSRootNode;
import org.iot.dsa.node.*;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.security.DSPasswordAes;

import java.beans.PropertyVetoException;
import java.sql.*;

import com.mchange.v2.c3p0.*;
import org.iot.dsa.time.DSDateTime;

public class DBConnectionNode extends DSNode {

    ///////////////////////////////////////////////////////////////////////////
    // Fields
    ///////////////////////////////////////////////////////////////////////////

    private final DSInfo db_name = getInfo(JDBCv2Helpers.DB_NAME);
    private final DSInfo db_url = getInfo(JDBCv2Helpers.DB_URL);
    private final DSInfo usr_name = getInfo(JDBCv2Helpers.DB_USER);
    private final DSInfo password = getInfo(JDBCv2Helpers.DB_PASSWORD);
    private final DSInfo driver = getInfo(JDBCv2Helpers.DRIVER);
    private final DSInfo conn_status = getInfo(JDBCv2Helpers.STATUS);
    private final DSInfo conn_succ = getInfo(JDBCv2Helpers.LAST_SUCCESS);
    private final DSInfo conn_fail = getInfo(JDBCv2Helpers.LAST_FAIL);
    private ComboPooledDataSource pool_data_source = null;

    ///////////////////////////////////////////////////////////////////////////
    // Methods - Constructors
    ///////////////////////////////////////////////////////////////////////////

    public DBConnectionNode () {

    }

    DBConnectionNode (DSMap params) {
        setParameters(params);
/*        put(db_name, params.get(JDBCv2Helpers.DB_NAME));
        put(db_url, params.get(JDBCv2Helpers.DB_URL));
        put(usr_name, params.get(JDBCv2Helpers.DB_USER));
        put(driver, params.get(JDBCv2Helpers.DRIVER));
        put(password, DSPasswordAes.valueOf(params.get(JDBCv2Helpers.DB_PASSWORD).toString()));*/
    }

    ///////////////////////////////////////////////////////////////////////////
    //Methods - Actions
    ///////////////////////////////////////////////////////////////////////////

    private DSAction makeQueryAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DBConnectionNode) info.getParent()).runQuery(invocation.getParameters(), this);
            }
        };
        act.addParameter(JDBCv2Helpers.QUERY, DSValueType.STRING, null);
        act.setResultType(ActionSpec.ResultType.CLOSED_TABLE);
        return act;
    }
    private ActionResult runQuery(DSMap params, DSAction act) {
        String query = params.get(JDBCv2Helpers.QUERY).toString();
        JDBCClosedTable res = null;
        Connection conn = null;
        try {
            conn = pool_data_source.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rSet = stmt.executeQuery(query);
            res = new JDBCClosedTable(act, rSet, getLogger());
            connSuccess(true);
        } catch (SQLException e) {
            connSuccess(false);
            warn("Failed to connect to Database: " + db_name.getValue(), e);
        }
        return res;
    }

    //TODO: Implement Streaming Queries
/*    private DSAction makeStreamingQueryAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DBConnectionNode) info.getParent()).runStreamingQuery(invocation, this);
            }
        };
        act.addParameter(JDBCv2Helpers.QUERY, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.INTERVAL, DSValueType.NUMBER, null);
        act.setResultType(ActionSpec.ResultType.STREAM_TABLE);
        return act;
    }
    private ActionResult runStreamingQuery(final ActionInvocation invoc, DSAction act) {
        DSMap params = invoc.getParameters();
        final String query = params.getString(JDBCv2Helpers.QUERY);
        Long interval = params.getLong(JDBCv2Helpers.INTERVAL);
        final Container<ResultSet> rSet = new Container<>();
        final Container<JDBCOpenTable> rTable = new Container<>();
        final Statement stmt;
        JDBCOpenTable res = null;
        DSRuntime.Timer stream = null;
        try {
            final String cursName = JDBCv2Helpers.randomCursorName();

            Connection conn = pool_data_source.getConnection();
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            stmt.execute("DECLARE " + "asdlkfjsa" + " CURSOR FOR " + query);

            rSet.setValue(stmt.executeQuery("FETCH NEXT FROM " + cursName));
            rTable.setValue(new JDBCOpenTable(act, rSet.getValue(), getLogger()));
            rTable.getValue().sendRows(rSet.getValue(), invoc);

            stream = DSRuntime.run(new Runnable() {
                @Override
                public void run() {
                    while (invoc.isOpen()) {
                        try {
                            rSet.setValue(stmt.executeQuery("FETCH NEXT FROM " + cursName));
                            rTable.getValue().sendRows(rSet.getValue(), invoc);
                        } catch (SQLException e) {
                            warn("Failed to fetch JDBCv2 SQL table: " + e);
                        }
                    }
                }
            }, 0, interval);

        } catch (SQLException e) {
            connSuccess(false);
            if (stream != null) {
                stream.cancel();
            }
            warn("Failed to connect to Database: " + db_name.getValue());
            warn(e);
        }
        return res;
    }*/

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

    private DSAction makeEditAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DBConnectionNode) info.getParent()).edit(invocation.getParameters());
            }
        };
        act.addParameter(JDBCv2Helpers.DB_NAME, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_URL, DSValueType.STRING, null).setPlaceHolder("jdbc:mysql://127.0.0.1:3306");
        act.addParameter(JDBCv2Helpers.DB_USER, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_PASSWORD, DSValueType.STRING, null).setEditor("password");
        DSList drivers = JDBCv2Helpers.getRegisteredDrivers();
        act.addParameter(JDBCv2Helpers.DRIVER, DSValueType.ENUM, null).setEnumRange(drivers);
        //TODO: add default timeout/poolable options
        //action.addParameter(new Parameter(JdbcConstants.DEFAULT_TIMEOUT, ValueType.NUMBER));
        //action.addParameter(new Parameter(JdbcConstants.POOLABLE, ValueType.BOOL, new Value(true)));
        return act;
    }
    private ActionResult edit(DSMap parameters) {
        setParameters(parameters);
        createDataPool();
        DSRootNode par = (DSRootNode) getParent();
        par.getLink().save();
        testDataPool();
        return null;
    }

    ///////////////////////////////////////////////////////////////////////////
    //Methods - Overrides
    ///////////////////////////////////////////////////////////////////////////

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        //Default Values
        declareDefault(JDBCv2Helpers.DB_NAME, DSString.valueOf("No Name"));
        declareDefault(JDBCv2Helpers.DB_USER, DSString.valueOf("No Name"));
        declareDefault(JDBCv2Helpers.DB_URL, DSString.valueOf("No URL"));
        declareDefault(JDBCv2Helpers.DRIVER, DSString.valueOf("No Driver"));
        declareDefault(JDBCv2Helpers.DB_PASSWORD, DSPasswordAes.valueOf("No Pass")).setHidden(true);
        declareDefault(JDBCv2Helpers.STATUS, DSString.valueOf(ConnStates.Unknown)).setReadOnly(true);
        declareDefault(JDBCv2Helpers.LAST_SUCCESS, DSString.valueOf("None")).setReadOnly(true);
        declareDefault(JDBCv2Helpers.LAST_FAIL, DSString.valueOf("None")).setReadOnly(true);
        //Default Actions
        declareDefault(JDBCv2Helpers.QUERY, makeQueryAction());
        declareDefault(JDBCv2Helpers.EDIT, makeEditAction());
        //TODO: Add streaming Queries
        //declareDefault(JDBCv2Helpers.STREAM_QUERY, makeStreamingQueryAction());
        declareDefault(JDBCv2Helpers.REMOVE, makeRemoveDatabaseAction());
    }

    @Override
    protected void onStable() {
        createDataPool();
    }

    ///////////////////////////////////////////////////////////////////////////
    //Methods - Helpers
    ///////////////////////////////////////////////////////////////////////////

    private void setParameters(DSMap params) {
        if (!params.isNull(JDBCv2Helpers.DB_NAME))
            put(db_name, params.get(JDBCv2Helpers.DB_NAME));
        if (!params.isNull(JDBCv2Helpers.DB_URL))
            put(db_url, params.get(JDBCv2Helpers.DB_URL));
        if (!params.isNull(JDBCv2Helpers.DB_USER))
            put(usr_name, params.get(JDBCv2Helpers.DB_USER));
        if (!params.isNull(JDBCv2Helpers.DRIVER))
            put(driver, params.get(JDBCv2Helpers.DRIVER));
        if (!params.isNull(JDBCv2Helpers.DB_PASSWORD))
            put(password, DSPasswordAes.valueOf(params.get(JDBCv2Helpers.DB_PASSWORD).toString()));
    }

    private void createDataPool() {
        if (pool_data_source != null)
            pool_data_source.close();
        try {
            String url = db_url.getValue().toString();
            String name = usr_name.getValue().toString();
            String pass = ((DSPasswordAes) password.getValue()).decode();
            String drvr = driver.getValue().toString();

            pool_data_source = new ComboPooledDataSource();
            pool_data_source.setDriverClass(drvr); //loads the jdbc driver
            pool_data_source.setJdbcUrl(url);
            pool_data_source.setUser(name);
            pool_data_source.setPassword(pass);
            pool_data_source.setAcquireRetryAttempts(6);
            pool_data_source.setAcquireRetryDelay(500);
            pool_data_source.setCheckoutTimeout(3000);
            //pool_data_source.setTestConnectionOnCheckout(true);
            //pool_data_source.setPreferredTestQuery("SELECT 1");

            //Alternative, uses standard JDBC drivers
            /*
            DataSource ds_unpooled = DataSources.unpooledDataSource(url, name, pass);
            DataSource ds_pooled = DataSources.pooledDataSource( ds_unpooled );
            */
        } catch (PropertyVetoException e) {
            connSuccess(false);
            warn("Failed to connect to Database: " + db_name.getValue() + " Message: " + e);
        }
        testDataPool();
    }

    private void connSuccess(boolean success) {
        DSDateTime stamp = DSDateTime.valueOf(System.currentTimeMillis());
        if (success) {
            put(conn_status, DSString.valueOf(ConnStates.Connected));
            put(conn_succ, stamp);
        } else {
            put(conn_status, DSString.valueOf(ConnStates.Failed));
            put(conn_fail, stamp);
        }
    }

    private void testDataPool() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            conn = pool_data_source.getConnection();
            stmt = conn.createStatement();
            res = stmt.executeQuery("SELECT 1");
            connSuccess(true);
        } catch (SQLException e) {
            connSuccess(false);
            warn("Failed to connect to Database: " + db_name.getValue(), e);
        } finally {
            if (conn != null) {
                JDBCv2Helpers.cleanClose(res, stmt, conn, getLogger());
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    //Helper Class
    ///////////////////////////////////////////////////////////////////////////

    public enum ConnStates {
        Connected,
        Failed,
        Unknown
    }
}
