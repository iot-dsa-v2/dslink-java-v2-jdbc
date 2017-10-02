package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.DSInt;
import org.iot.dsa.node.DSNode;

public class DBConnectionNode extends DSNode {

    @Override
    protected void declareDefaults() {

    }

    @Override
    protected void onStable() {
        put("Number", DSInt.valueOf(42));
    }
}
