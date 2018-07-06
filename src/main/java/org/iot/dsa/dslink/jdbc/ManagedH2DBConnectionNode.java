package org.iot.dsa.dslink.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.tools.Server;
import org.iot.dsa.dslink.DSMainNode;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.DSAction;

/**
 * Class designed for handling user-friendly simple local H2 databases.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
@SuppressWarnings("SqlNoDataSourceInspection")
public class ManagedH2DBConnectionNode extends DBConnectionNode {

    private final DSInfo extrnl = getInfo(JDBCv2Helpers.EXT_ACCESS);
    private static String NO_URL = "No Access";
    private boolean driver_loaded = false;
    private Server server;

    @SuppressWarnings("unused")
    public ManagedH2DBConnectionNode() {

    }

    ManagedH2DBConnectionNode(DSMap params) {
        super(params);
    }

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault(JDBCv2Helpers.EXT_ACCESS, DSBool.make(false));
        declareDefault(JDBCv2Helpers.SHOW_TABLES, makeShowTablesAction());
    }

    @Override
    protected void onChildChanged(DSInfo info) {
        super.onChildChanged(info);
        if (info.getName().equals(JDBCv2Helpers.EXT_ACCESS)) {
            if (info.getValue().toElement().toBoolean()) {
                startTCPServer();
            } else {
                stopTCPServer();
            }
        }
    }

    @Override
    protected void onStable() {
        super.onStable();
        //TODO: Ask AAron if this is the best way to get boolean
        if (extrnl.getValue().toElement().toBoolean()) {
            startTCPServer();
        } else {
            stopTCPServer();
        }
    }

    @Override
    void closeConnections() {
        stopTCPServer();
    }

    @Override
    void createDatabaseConnection() {
        if (extrnl.getValue().toElement().toBoolean()) {
            startTCPServer();
        }
    }

    @Override
    ActionResult edit(DSMap parameters) {
        DSElement newUsr = parameters.get(JDBCv2Helpers.DB_USER);
        DSElement newPass = parameters.get(JDBCv2Helpers.DB_PASSWORD);
        //noinspection UnusedAssignment
        String newUsrStr = null;
        String curUserStr = usr_name.getValue().toString();

        Connection data = null;
        Statement chg_usr = null;
        Statement chg_pass = null;

        try {
            data = getConnection();
            try {
                if (newUsr != null) {
                    newUsrStr = newUsr.toString();
                    if (!newUsrStr.toUpperCase().equals(curUserStr.toUpperCase())) {
                        chg_usr = data.createStatement();
                        chg_usr.execute("ALTER USER " + curUserStr + " RENAME TO " + newUsrStr);
                        data.commit();
                    }
                } else {
                    newUsrStr = curUserStr;
                }

                if (newPass == null) {
                    newPass = DSElement.make(getCurPass());
                }

                //Password must always be re-set after change of user name (JDBC quirk)
                chg_pass = data.createStatement();
                chg_pass.execute(
                        "ALTER USER " + newUsrStr + " SET PASSWORD '" + newPass.toString() + "'");
                data.commit();

            } catch (Exception ex) {
                warn("User/Pass change error:", ex);
            }
        } catch (SQLException e) {
            warn("Failed to get connection.", e);
            connSuccess(false);
        } finally {
            JDBCv2Helpers.cleanClose(null, chg_pass, data, this);
            JDBCv2Helpers.cleanClose(null, chg_usr, data, this);
        }

        setParameters(parameters);
        testConnection();
        DSMainNode par = (DSMainNode) getParent();
        par.getLink().save();
        return null;
    }

    @Override
    Connection getConnection() throws SQLException {
        try {
            updateServerURL();
            return DriverManager.getConnection("jdbc:h2:" + getCurDBName(),
                                               usr_name.getValue().toString(),
                                               getCurPass());
        } catch (Exception x) {
            warn("Failed to login:", x);
        }
        return null;
    }

    private String getCurDBName() {
        return "./db/" + db_name.getValue().toString();
    }

    private String getServerURL() {
        return (server != null) ? "jdbc:h2:" + server.getURL() + "/" + getCurDBName() : NO_URL;
    }

    private DSAction makeShowTablesAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                invocation.getParameters().put(JDBCv2Helpers.QUERY, "SHOW TABLES");
                DBConnectionNode par = (DBConnectionNode) info.getParent();
                ResultSet res = par.executeQuery("SHOW TABLES");
                if (invocation.getParameters().get(JDBCv2Helpers.MAKE_NODES).toBoolean()) {
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
                return ((DBConnectionNode) info.getParent())
                        .runQuery(invocation.getParameters(), this);
            }
        };
        act.addParameter(JDBCv2Helpers.MAKE_NODES, DSValueType.BOOL, null)
           .setDefault(DSElement.make(false));
        act.setResultType(ActionSpec.ResultType.CLOSED_TABLE);
        return act;
    }

    private void startTCPServer() {
        try {
            server = Server.createTcpServer("-tcpAllowOthers").start();
        } catch (SQLException e) {
            warn("Cannot start Web Server", e);
        }
        updateServerURL();
    }

    private void stopTCPServer() {
        if (server != null) {
            server.stop();
        }
        server = null;
        put(db_url, DSElement.make(NO_URL));
    }

    private void updateServerURL() {
        put(db_url, DSElement.make(getServerURL()));
    }
}
