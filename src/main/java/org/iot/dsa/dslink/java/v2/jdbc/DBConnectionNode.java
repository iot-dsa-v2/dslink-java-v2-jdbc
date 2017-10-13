package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.dslink.DSRootNode;
import org.iot.dsa.node.*;
import org.iot.dsa.node.action.*;
import org.iot.dsa.security.DSPasswordAes;
import org.iot.dsa.time.DSDateTime;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

abstract public class DBConnectionNode extends DSNode {

    ///////////////////////////////////////////////////////////////////////////
    // Fields
    ///////////////////////////////////////////////////////////////////////////

    final DSInfo db_name = getInfo(JDBCv2Helpers.DB_NAME);
    final DSInfo db_url = getInfo(JDBCv2Helpers.DB_URL);
    final DSInfo usr_name = getInfo(JDBCv2Helpers.DB_USER);
    final DSInfo password = getInfo(JDBCv2Helpers.DB_PASSWORD);
    final DSInfo driver = getInfo(JDBCv2Helpers.DRIVER);
    private final DSInfo conn_status = getInfo(JDBCv2Helpers.STATUS);
    private final DSInfo conn_succ = getInfo(JDBCv2Helpers.LAST_SUCCESS);
    private final DSInfo conn_fail = getInfo(JDBCv2Helpers.LAST_FAIL);

    ///////////////////////////////////////////////////////////////////////////
    // Methods - Constructors
    ///////////////////////////////////////////////////////////////////////////

    @SuppressWarnings("WeakerAccess")
    public DBConnectionNode() {

    }

    DBConnectionNode(DSMap params) {
        setParameters(params);
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
        Connection conn = null;
        Statement stmt = null;
        ResultSet rSet = null;
        @SuppressWarnings("UnusedAssignment") JDBCClosedTable res = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            connSuccess(true);
            try {
                rSet = stmt.executeQuery(query);
                res = new JDBCClosedTable(act, rSet, getLogger());
            } catch (SQLException e) {
                put(conn_status, DSString.valueOf(ConnStates.Unknown));
                JDBCv2Helpers.cleanClose(rSet, stmt, conn, getLogger());
                throw new DSRequestException("Query failed: " + e);
            }
        } catch (SQLException e) {
            connSuccess(false);
            //noinspection ConstantConditions
            JDBCv2Helpers.cleanClose(rSet, stmt, conn, getLogger());
            warn("Failed to connect to Database: " + db_name.getValue(), e);
            throw new DSRequestException("Database connection failed: " + e);
        }
        return res;
    }

    private DSAction makeUpdateAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DBConnectionNode) info.getParent()).runUpdate(invocation.getParameters());
            }
        };
        act.addParameter(JDBCv2Helpers.QUERY, DSValueType.STRING, null);
        return act;
    }

    private ActionResult runUpdate(DSMap params) {
        String query = params.get(JDBCv2Helpers.QUERY).toString();
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            connSuccess(true);
            try {
                stmt.executeUpdate(query);
            } catch (SQLException e) {
                put(conn_status, DSString.valueOf(ConnStates.Unknown));
                JDBCv2Helpers.cleanClose(null, stmt, conn, getLogger());
                throw new DSRequestException("Update failed: " + e);
            }
        } catch (SQLException e) {
            connSuccess(false);
            //noinspection ConstantConditions
            JDBCv2Helpers.cleanClose(null, stmt, conn, getLogger());
            warn("Failed to connect to Database: " + db_name.getValue(), e);
            throw new DSRequestException("Database connection failed: " + e);
        }
        return null;
    }

    abstract Connection getConnection() throws SQLException;

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
        closeConnections();
        getParent().remove(getInfo());
    }

    abstract void closeConnections();

    DSAction makeEditAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DBConnectionNode) info.getParent()).edit(invocation.getParameters());
            }
        };
        act.addParameter(JDBCv2Helpers.DB_NAME, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_USER, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_PASSWORD, DSValueType.STRING, null).setEditor("password");
        //TODO: add default timeout/poolable options
        //action.addParameter(new Parameter(JdbcConstants.DEFAULT_TIMEOUT, ValueType.NUMBER));
        //action.addParameter(new Parameter(JdbcConstants.POOLABLE, ValueType.BOOL, new Value(true)));
        return act;
    }

    ActionResult edit(DSMap parameters) {
        setParameters(parameters);
        closeConnections();
        createDatabaseConnection();
        DSRootNode par = (DSRootNode) getParent();
        par.getLink().save();
        testConnection();
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
        declareDefault(JDBCv2Helpers.UPDATE, makeUpdateAction());
        //TODO: Add streaming Queries
        //declareDefault(JDBCv2Helpers.STREAM_QUERY, makeStreamingQueryAction());
        declareDefault(JDBCv2Helpers.REMOVE, makeRemoveDatabaseAction());
    }

    @Override
    protected void onStable() {
        createDatabaseConnection();
    }

    abstract void createDatabaseConnection();

    ///////////////////////////////////////////////////////////////////////////
    //Methods - Helpers
    ///////////////////////////////////////////////////////////////////////////

    void setParameters(DSMap params) {
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



    void connSuccess(boolean success) {
        DSDateTime stamp = DSDateTime.valueOf(System.currentTimeMillis());
        if (success) {
            put(conn_status, DSString.valueOf(ConnStates.Connected));
            put(conn_succ, stamp);
        } else {
            put(conn_status, DSString.valueOf(ConnStates.Failed));
            put(conn_fail, stamp);
        }
    }

    void testConnection() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            //noinspection SqlNoDataSourceInspection
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
