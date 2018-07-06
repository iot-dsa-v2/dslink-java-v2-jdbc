package org.iot.dsa.dslink.jdbc;

import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.DSAction;

/**
 * Node representing a table in a managed H2 database.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
public class TableNode extends DSNode {

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault(JDBCv2Helpers.SHOW_TABLE, makeShowTableAction());
    }

    private DSAction makeShowTableAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                String tableName = info.getParent().getName();
                invocation.getParameters().put(JDBCv2Helpers.QUERY, "SELECT * FROM " + tableName);
                DBConnectionNode connNode = (DBConnectionNode) info.getParent().getParent();
                return connNode.runQuery(invocation.getParameters(), this);
            }
        };
        act.setResultType(ActionSpec.ResultType.CLOSED_TABLE);
        return act;
    }
}
