package org.iot.dsa.dslink.java.v2.jdbc;

import org.h2.tools.Server;
import org.iot.dsa.dslink.DSRootNode;
import org.iot.dsa.node.*;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.security.DSPasswordAes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@SuppressWarnings("SqlNoDataSourceInspection")
public class ManagedH2DBConnectionNode extends DBConnectionNode {

    private final DSInfo extrnl = getInfo(JDBCv2Helpers.EXT_ACCESS);
    private static String NO_URL = "No Access";
    private Server server;

    ///////////////////////////////////////////////////////////////////////////
    // Methods - Constructors
    ///////////////////////////////////////////////////////////////////////////

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
    }

    @Override
    Connection getConnection() throws SQLException {
        //TODO: Ask AAron if this is the best way to get boolean
        if (extrnl.getValue().toElement().toBoolean()) {
            startTCPServer();
        }

        System.out.println("jdbc:h2:" + getCurDBName());
        System.out.println(usr_name.getValue().toString());
        System.out.println(((DSPasswordAes) password.getValue()).decode());
        try {
            updateServerURL();
            return DriverManager.getConnection("jdbc:h2:" + getCurDBName(), //"jdbc:h2:~/test"
                    usr_name.getValue().toString(), //"sa"
                    ((DSPasswordAes) password.getValue()).decode()); //""
        } catch (Exception x) {
            warn("Failed to login:", x);
        }
        return null;
    }

    private void startTCPServer() {
        try {
            server = Server.createTcpServer("-tcpAllowOthers").start();
        } catch (SQLException e) {
            warn("Cannot start Web Server", e);
        }
        updateServerURL();
    }

    private void updateServerURL() {
        put(db_url, DSElement.make(getServerURL()));
    }

    private String getServerURL() {
        return (server != null) ? "jdbc:h2:" + server.getURL() + "/" + getCurDBName() : NO_URL;
    }

    private String getCurDBName() {
        return "./db/" + db_name.getValue().toString();
    }

    private void stopTCPServer() {
        if (server != null) server.stop();
        server = null;
        put(db_url, DSElement.make(NO_URL));
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

        //TODO: figure out why a missing login or pass field breaks the connection
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

                 if (newPass != null) {
                     chg_pass = data.createStatement();
                     chg_pass.execute("ALTER USER " + newUsrStr + " SET PASSWORD '" + newPass.toString() + "'");
                     data.commit();
                 }
            } catch (Exception ex) {
                warn("User/Pass change error:", ex);
            }
        } catch (SQLException e) {
            warn("Failed to get connection.", e);
            connSuccess(false);
        } finally {
            JDBCv2Helpers.cleanClose(null, chg_pass, data, getLogger());
            JDBCv2Helpers.cleanClose(null, chg_usr, data, getLogger());
        }

        setParameters(parameters);
        testConnection();
        DSRootNode par = (DSRootNode) getParent();
        par.getLink().save();
        return null;
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
    void closeConnections() {
        stopTCPServer();
    }

    @Override
    void createDatabaseConnection() {
        if (extrnl.getValue().toElement().toBoolean()) {
            startTCPServer();
        }
    }
}
