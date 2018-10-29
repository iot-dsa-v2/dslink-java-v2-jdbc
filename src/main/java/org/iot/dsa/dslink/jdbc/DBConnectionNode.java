package org.iot.dsa.dslink.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.iot.dsa.conn.DSBaseConnection;
import org.iot.dsa.dslink.DSMainNode;
import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.security.DSPasswordAes128;

/**
 * Generic connection node designed to handle any type of database connection.
 *
 * @author Juris Puchin
 * Created on 10/13/2017
 */
abstract public class DBConnectionNode extends DSBaseConnection {

    ///////////////////////////////////////////////////////////////////////////
    // Fields
    ///////////////////////////////////////////////////////////////////////////

    final DSInfo db_name = getInfo(JDBCv2Helpers.DB_NAME);
    final DSInfo db_url = getInfo(JDBCv2Helpers.DB_URL);
    final DSInfo usr_name = getInfo(JDBCv2Helpers.DB_USER);
    final DSInfo password = getInfo(JDBCv2Helpers.DB_PASSWORD);
    final DSInfo driver = getInfo(JDBCv2Helpers.DRIVER);
//    private final DSInfo conn_status = getInfo(JDBCv2Helpers.STATUS);
    private final DSInfo conn_succ = getInfo(JDBCv2Helpers.LAST_SUCCESS);
    private final DSInfo conn_fail = getInfo(JDBCv2Helpers.LAST_FAIL);
    private final DSInfo enabled = getInfo(JDBCv2Helpers.ENABLED);

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    @SuppressWarnings("WeakerAccess")
    public DBConnectionNode() {

    }

    DBConnectionNode(DSMap params) {
        setParameters(params);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Methods
    ///////////////////////////////////////////////////////////////////////////
    
    @Override
    public boolean isEnabled() {
        return enabled.getElement().toBoolean();
    }

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        //Default Values
        declareDefault(JDBCv2Helpers.ENABLED, DSBool.TRUE);
        declareDefault(JDBCv2Helpers.DB_NAME, DSString.valueOf("No Name"));
        declareDefault(JDBCv2Helpers.DB_USER, DSString.valueOf("No Name"));
        declareDefault(JDBCv2Helpers.DB_URL, DSString.valueOf("No URL"));
        declareDefault(JDBCv2Helpers.DRIVER, DSString.valueOf("No Driver"));
        declareDefault(JDBCv2Helpers.DB_PASSWORD, DSPasswordAes128.valueOf("No Pass"))
                .setHidden(true);
//        declareDefault(JDBCv2Helpers.STATUS, DSString.valueOf(ConnStates.Unknown))
//                .setReadOnly(true);
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
    protected void onChildChanged(DSInfo child) {
        if (child == enabled) {
            canConnect(); //update disabled status
        }
        super.onChildChanged(child);
    }

    @Override
    protected void onStable() {
        createDatabaseConnection();
    }

    abstract void closeConnections();

//    void connSuccess(boolean success) {
//        DSDateTime stamp = DSDateTime.valueOf(System.currentTimeMillis());
//        if (success) {
//            put(conn_status, DSString.valueOf(ConnStates.Connected));
//            put(conn_succ, stamp);
//            connOk();
//        } else {
//            put(conn_status, DSString.valueOf(ConnStates.Failed));
//            put(conn_fail, stamp);
//            connDown("");
//        }
//    }

    abstract void createDatabaseConnection();

    ActionResult edit(DSMap parameters) {
        setParameters(parameters);
        closeConnections();
        createDatabaseConnection();
        DSMainNode par = (DSMainNode) getParent();
        testConnection();
        return null;
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
            rTable.setValue(new JDBCOpenTable(act, rSet.getValue(), this));
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

    ResultSet executeQuery(String sqlQuery) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rSet = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
//            connSuccess(true);
            connOk();
            try {
                rSet = stmt.executeQuery(sqlQuery);
            } catch (SQLException e) {
//                put(conn_status, DSString.valueOf(ConnStates.Unknown));
                JDBCv2Helpers.cleanClose(null, stmt, conn, this);
                throw new DSRequestException("Query failed: " + e);
            }
        } catch (SQLException e) {
//            connSuccess(false);
            connDown(e.getMessage());
            //noinspection ConstantConditions
            JDBCv2Helpers.cleanClose(rSet, stmt, conn, this);
            warn("Failed to connect to Database: " + db_name.getValue(), e);
            throw new DSRequestException("Database connection failed: " + e);
        }
        return rSet;
    }

    abstract Connection getConnection() throws SQLException;

    String getCurPass() {
        return ((DSPasswordAes128) password.getValue()).decode();
    }

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

    private DSAction makeQueryAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((DBConnectionNode) info.getParent())
                        .runQuery(invocation.getParameters(), this);
            }
        };
        act.addParameter(JDBCv2Helpers.QUERY, DSValueType.STRING, null);
        act.setResultType(ActionSpec.ResultType.CLOSED_TABLE);
        return act;
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

    private void removeDatabase() {
        closeConnections();
        getParent().remove(getInfo());
    }

    ActionResult runQuery(DSMap params, DSAction act) {
        String query = params.get(JDBCv2Helpers.QUERY).toString();
        ResultSet rSet = executeQuery(query);
        ActionResult res;
        try {
            res = new JDBCClosedTable(act, rSet, this);
        } catch (SQLException e) {
//            put(conn_status, DSString.valueOf(ConnStates.Unknown));
            JDBCv2Helpers.cleanClose(rSet, null, null, this);
            throw new DSRequestException("Failed to retrieve data from database: " + e);
        }
        return res;
    }

    private ActionResult runUpdate(DSMap params) {
        String query = params.get(JDBCv2Helpers.QUERY).toString();
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
//            connSuccess(true);
            connOk();
            try {
                stmt.executeUpdate(query);
            } catch (SQLException e) {
//                put(conn_status, DSString.valueOf(ConnStates.Unknown));
                JDBCv2Helpers.cleanClose(null, stmt, conn, this);
                throw new DSRequestException("Update failed: " + e);
            }
        } catch (SQLException e) {
//            connSuccess(false);
            connDown(e.getMessage());
            //noinspection ConstantConditions
            JDBCv2Helpers.cleanClose(null, stmt, conn, this);
            warn("Failed to connect to Database: " + db_name.getValue(), e);
            throw new DSRequestException("Database connection failed: " + e);
        }
        return null;
    }

    private void setCurPass(String pass) {
        put(password, DSPasswordAes128.valueOf(pass));
    }

    void setParameters(DSMap params) {
        if (!params.isNull(JDBCv2Helpers.DB_NAME)) {
            put(db_name, params.get(JDBCv2Helpers.DB_NAME));
        }
        if (!params.isNull(JDBCv2Helpers.DB_URL)) {
            put(db_url, params.get(JDBCv2Helpers.DB_URL));
        }
        if (!params.isNull(JDBCv2Helpers.DB_USER)) {
            put(usr_name, params.get(JDBCv2Helpers.DB_USER));
        }
        if (!params.isNull(JDBCv2Helpers.DRIVER)) {
            put(driver, params.get(JDBCv2Helpers.DRIVER));
        }
        if (!params.isNull(JDBCv2Helpers.DB_PASSWORD)) {
            setCurPass(params.get(JDBCv2Helpers.DB_PASSWORD).toString());
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
//            connSuccess(true);
            connOk();
        } catch (SQLException e) {
//            connSuccess(false);
            connDown(e.getMessage());
            warn("Failed to connect to Database: " + db_name.getValue(), e);
        } finally {
            if (conn != null) {
                JDBCv2Helpers.cleanClose(res, stmt, conn, this);
            } else {
//                connSuccess(false);
                connDown("Conn is null");
            }
        }
    }

//    public enum ConnStates {
//        Connected,
//        Failed,
//        Unknown
//    }
}
