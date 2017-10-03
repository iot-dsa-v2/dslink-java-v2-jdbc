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
        declareDefault(JDBCv2Helpers.DB_NAME, DSString.valueOf("No Name"));
        declareDefault(JDBCv2Helpers.REMOVE, makeRemoveDatabaseAction());
    }

    @Override
    protected void onStable() {

    }
}
