package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.*;

public class DBConnectionNode extends DSNode {

    private final DSInfo db_name = getInfo(JDBCv2Helpers.DB_NAME);

    public DBConnectionNode () {
    }

    DBConnectionNode (DSMap params) {
        put(db_name, params.get(JDBCv2Helpers.DB_NAME));
    }

    @Override
    protected void declareDefaults() {
        declareDefault(JDBCv2Helpers.DB_NAME, DSString.valueOf("No Name"));
    }

    @Override
    protected void onStable() {

    }
}
