package org.iot.dsa.dslink.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.iot.dsa.conn.DSBaseConnection;
import org.iot.dsa.dslink.Action.ResultsType;
import org.iot.dsa.dslink.ActionResults;
import org.iot.dsa.dslink.DSRequestException;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSLong;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.node.action.DSIActionRequest;
import org.iot.dsa.security.DSPasswordAes128;

/**
 * Generic connection node designed to handle any type of database connection.
 *
 * @author Juris Puchin
 * Created on 10/13/2017
 */
abstract public class DBConnectionNode extends DSBaseConnection implements JDBCObject {

    ///////////////////////////////////////////////////////////////////////////
    // Class Fields
    ///////////////////////////////////////////////////////////////////////////

    public static final String CREATE_NODES = "Create Nodes";
    public static final String PREPARED_QUERY = "Prepared Query";
    public static final String PREPARED_UPDATE = "Prepared Update";
    public static final String QUERY = "Query";
    public static final String SHOW_TABLES = "Show Tables";
    public static final String UPDATE = "Update";

    ///////////////////////////////////////////////////////////////////////////
    // Instance Fields
    ///////////////////////////////////////////////////////////////////////////

    protected final DSInfo db_name = getInfo(DB_NAME);
    protected final DSInfo db_url = getInfo(DB_URL);
    protected final DSInfo driver = getInfo(DRIVER);
    protected final DSInfo password = getInfo(DB_PASSWORD);
    protected final DSInfo usr_name = getInfo(DB_USER);

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    @SuppressWarnings("WeakerAccess")
    public DBConnectionNode() {

    }

    protected DBConnectionNode(DSMap params) {
        setParameters(params);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Public Methods
    ///////////////////////////////////////////////////////////////////////////

    public ResultSet executeQuery(String statement) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            connOk();
            try {
                res = stmt.executeQuery(statement);
            } catch (SQLException e) {
                cleanClose(res, stmt, conn, this);
                throw new DSRequestException("Query failed: " + e);
            }
        } catch (SQLException e) {
            connDown(e.getMessage());
            //noinspection ConstantConditions
            cleanClose(res, stmt, conn, this);
            warn("Failed to connect to Database: " + db_name.getValue(), e);
            throw new DSRequestException("Database connection failed: " + e);
        }
        return res;
    }

    public void executeUpdate(String statement) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            connOk();
            try {
                stmt.executeUpdate(statement);
            } catch (SQLException e) {
                cleanClose(null, stmt, conn, this);
                throw new DSRequestException("Update failed: " + e);
            }
        } catch (SQLException e) {
            connDown(e.getMessage());
            //noinspection ConstantConditions
            cleanClose(null, stmt, conn, this);
            warn("Failed to connect to Database: " + db_name.getValue(), e);
            throw new DSRequestException("Database connection failed: " + e);
        }
        cleanClose(null, stmt, conn, this);
    }

    public ActionResults runQuery(DSIActionRequest req) {
        String query = req.getParameters().get(STATEMENT).toString();
        ResultSet rSet = executeQuery(query);
        ActionResults res;
        try {
            res = new JDBCTable(req, rSet, this);
        } catch (SQLException e) {
            cleanClose(rSet, null, null, this);
            throw new DSRequestException("Failed to retrieve data from database: " + e);
        }
        return res;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Protected Methods
    ///////////////////////////////////////////////////////////////////////////

    protected abstract void closeConnections();

    protected abstract void createDatabaseConnection();

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        //Default Values
        declareDefault(DB_NAME, DSString.valueOf("No Name"));
        declareDefault(DB_USER, DSString.valueOf("No Name"));
        declareDefault(DB_URL, DSString.valueOf("No URL"));
        declareDefault(DRIVER, DSString.valueOf("No Driver"));
        declareDefault(DB_PASSWORD, DSPasswordAes128.valueOf("No Pass"));
        //Default Actions
        declareDefault(QUERY, new DSAction() {
                           @Override
                           public ActionResults invoke(DSIActionRequest req) {
                               return ((DBConnectionNode) req.getTarget()).runQuery(req);
                           }

                           {
                               addParameter(STATEMENT, DSString.NULL, null);
                               setResultsType(ResultsType.TABLE);
                           }
                       }
        );
        declareDefault(UPDATE, makeUpdateAction());
        declareDefault(SHOW_TABLES, makeShowTablesAction());
        declareDefault(SETTINGS, makeEditAction()).getMetadata().setActionGroup(
                DSAction.EDIT_GROUP, SETTINGS);
        declareDefault(PREPARED_QUERY, makeAddPreparedQueryAction(), null)
                .getMetadata().setActionGroup(DSAction.NEW_GROUP, null);
        declareDefault(PREPARED_UPDATE, makeAddPreparedUpdateAction(), null)
                .getMetadata().setActionGroup(DSAction.NEW_GROUP, null);
    }

    protected ActionResults edit(DSMap parameters) {
        setParameters(parameters);
        closeConnections();
        createDatabaseConnection();
        testConnection();
        return null;
    }

    protected abstract Connection getConnection() throws SQLException;

    protected String getCurPass() {
        return ((DSPasswordAes128) password.getValue()).decode();
    }

    protected DSAction makeAddPreparedQueryAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest request) {
                DSMap params = request.getParameters();
                String name = params.get(NAME, null);
                if ((name == null) || name.isEmpty()) {
                    throw new IllegalArgumentException("Missing name");
                }
                if (getInfo(name) != null) {
                    throw new IllegalArgumentException("Name already in use: " + name);
                }
                String statement = params.get(AbstractPreparedStatement.STATEMENT, "");
                int count = params.get(AbstractPreparedStatement.PARAMETERS, 0);
                JDBCPreparedQuery node = new JDBCPreparedQuery();
                request.getTargetInfo().getNode().put(name, node);
                node.setParameterCount(count);
                node.setStatement(statement);
                return null;
            }
        };
        act.addParameter(NAME, DSString.EMPTY, "Node name");
        act.addParameter(AbstractPreparedStatement.STATEMENT, DSString.EMPTY, "Prepared statement");
        act.addDefaultParameter(AbstractPreparedStatement.PARAMETERS, DSLong.valueOf(0),
                                "Number of parameters in the statement");
        return act;
    }

    protected DSAction makeAddPreparedUpdateAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest request) {
                DSMap params = request.getParameters();
                String name = params.get(NAME, null);
                if ((name == null) || name.isEmpty()) {
                    throw new IllegalArgumentException("Missing name");
                }
                if (getInfo(name) != null) {
                    throw new IllegalArgumentException("Name already in use: " + name);
                }
                String statement = params.get(AbstractPreparedStatement.STATEMENT, "");
                int count = params.get(AbstractPreparedStatement.PARAMETERS, 0);
                JDBCPreparedUpdate node = new JDBCPreparedUpdate();
                request.getTargetInfo().getNode().put(name, node);
                node.setParameterCount(count);
                node.setStatement(statement);
                return null;
            }
        };
        act.addParameter(NAME, DSString.EMPTY, "Node name");
        act.addParameter(AbstractPreparedStatement.STATEMENT, DSString.EMPTY, "Prepared statement");
        act.addDefaultParameter(AbstractPreparedStatement.PARAMETERS, DSLong.valueOf(0),
                                "Number of parameters in the statement");
        return act;
    }

    protected DSAction makeEditAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest req) {
                return ((DBConnectionNode) req.getTarget()).edit(req.getParameters());
            }
        };
        act.addParameter(DB_NAME, DSString.NULL, null);
        act.addParameter(DB_USER, DSString.NULL, null);
        act.addParameter(DB_PASSWORD, DSString.NULL, null).setEditor("password");
        return act;
    }

    protected DSAction makeUpdateAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest req) {
                String query = req.getParameters().get(STATEMENT).toString();
                ((DBConnectionNode) req.getTarget()).executeUpdate(query);
                return null;
            }
        };
        act.addParameter(STATEMENT, DSString.NULL, null);
        return act;
    }

    @Override
    protected void onStable() {
        super.onStable();
        createDatabaseConnection();
    }

    @Override
    protected void onStopped() {
        closeConnections();
        super.onStopped();
    }

    protected void setParameters(DSMap params) {
        if (!params.isNull(DB_NAME)) {
            put(db_name, params.get(DB_NAME));
        }
        if (!params.isNull(DB_URL)) {
            put(db_url, params.get(DB_URL));
        }
        if (!params.isNull(DB_USER)) {
            put(usr_name, params.get(DB_USER));
        }
        if (!params.isNull(DRIVER)) {
            put(driver, params.get(DRIVER));
        }
        if (!params.isNull(DB_PASSWORD)) {
            setCurPass(params.get(DB_PASSWORD).toString());
        }
    }

    protected void testConnection() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            //noinspection SqlNoDataSourceInspection
            res = stmt.executeQuery("SELECT 1");
            connOk();
        } catch (SQLException e) {
            connDown(e.getMessage());
            warn("Failed to connect to Database: " + db_name.getValue(), e);
        } finally {
            if (conn != null) {
                cleanClose(res, stmt, conn, this);
            } else {
                connDown("Conn is null");
            }
        }
    }

    private DSAction makeShowTablesAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest req) {
                req.getParameters().put(STATEMENT, "SHOW TABLES");
                DBConnectionNode par = (DBConnectionNode) req.getTarget();
                ResultSet res = par.executeQuery("SHOW TABLES");
                if (req.getParameters().get(CREATE_NODES).toBoolean()) {
                    try {
                        while (res.next()) {
                            String nxtNode = res.getString(1);
                            if (par.get(nxtNode) == null) {
                                par.add(nxtNode, new TableNode());
                            }
                        }
                    } catch (SQLException e) {
                        warn("Failed to read table list: ", e);
                    }
                }
                return ((DBConnectionNode) req.getTarget()).runQuery(req);
            }
        };
        act.addParameter(CREATE_NODES, DSBool.TRUE, null)
           .setDefault(DSElement.make(false));
        act.setResultsType(ResultsType.TABLE);
        return act;
    }

    private void setCurPass(String pass) {
        put(password, DSPasswordAes128.valueOf(pass));
    }
}
