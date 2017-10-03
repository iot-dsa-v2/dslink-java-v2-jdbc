package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.*;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;

public class DBConnectionNode extends DSNode {

    private final DSInfo db_name = getInfo(JDBCv2Helpers.DB_NAME);

    public DBConnectionNode () {
    }

    DBConnectionNode (DSMap params) {
        put(db_name, params.get(JDBCv2Helpers.DB_NAME));
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
        getParent().remove(getInfo());
    }

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        //Default Values
        declareDefault(JDBCv2Helpers.DB_NAME, DSString.valueOf("No Name"));
        declareDefault(JDBCv2Helpers.STATUS, DSString.valueOf(ConnStates.Unknown));
        //Default Actions
        declareDefault(JDBCv2Helpers.QUERY, makeQueryAction());
        declareDefault(JDBCv2Helpers.REMOVE, makeRemoveDatabaseAction());
    }

    @Override
    protected void onStable() {

    }

    public enum ConnStates {
        Connected,
        Disconnected,
        Unknown
    }
}
