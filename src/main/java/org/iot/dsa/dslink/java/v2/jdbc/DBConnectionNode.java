package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.DSRuntime;
import org.iot.dsa.node.*;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.security.DSPasswordAes;

import java.beans.PropertyVetoException;
import java.sql.*;

import com.mchange.v2.c3p0.*;

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
    private ComboPooledDataSource pool_data_source = null;

    ///////////////////////////////////////////////////////////////////////////
    // Methods - Constructors
    ///////////////////////////////////////////////////////////////////////////

    public DBConnectionNode () {

    }

    DBConnectionNode (DSMap params) {
        put(db_name, params.get(JDBCv2Helpers.DB_NAME));
        put(db_url, params.get(JDBCv2Helpers.DB_URL));
        put(usr_name, params.get(JDBCv2Helpers.DB_USER));
        put(driver, params.get(JDBCv2Helpers.DRIVER));
        put(password, DSPasswordAes.valueOf(params.get(JDBCv2Helpers.DB_PASSWORD).toString()));
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
        try {
            Connection conn = pool_data_source.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rSet = stmt.executeQuery(query);
            System.out.println(rSet.toString());
            res = new JDBCClosedTable(act, rSet);
        } catch (SQLException e) {
            put(conn_status, DSString.valueOf(ConnStates.Failed));
            warn("Failed to connect to Database: " + db_name.getValue());
            warn(e);
        }
        return res;
    }

    //TODO: Implement Streaming Queries
    private DSAction makeStreamingQueryAction() {
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
            rTable.setValue(new JDBCOpenTable(act, rSet.getValue()));
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
            put(conn_status, DSString.valueOf(ConnStates.Failed));
            if (stream != null) {
                stream.cancel();
            }
            warn("Failed to connect to Database: " + db_name.getValue());
            warn(e);
        }
        return res;
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
        //Default Actions
        declareDefault(JDBCv2Helpers.QUERY, makeQueryAction());
        //TODO: Add streaming Queries
        //declareDefault(JDBCv2Helpers.STREAM_QUERY, makeStreamingQueryAction());
        declareDefault(JDBCv2Helpers.REMOVE, makeRemoveDatabaseAction());
    }

    @Override
    protected void onStable() {
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

            //Alternative, uses standard JDBC drivers
            /*
            DataSource ds_unpooled = DataSources.unpooledDataSource(url, name, pass);
            DataSource ds_pooled = DataSources.pooledDataSource( ds_unpooled );
            */

            put(conn_status, DSString.valueOf(ConnStates.Connected));
        } catch (PropertyVetoException e) {
            put(conn_status, DSString.valueOf(ConnStates.Failed));
            warn("Failed to connect to Database: " + db_name.getValue() + " Message: " + e);
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
