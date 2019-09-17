package org.iot.dsa.dslink.jdbc;

import org.iot.dsa.dslink.Action.ResultsType;
import org.iot.dsa.dslink.ActionResults;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.node.action.DSIActionRequest;

/**
 * Node representing a table in a managed database.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
public class TableNode extends DSNode implements JDBCObject {

    String SHOW_TABLE = "Show Table";

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault(SHOW_TABLE, makeShowTableAction());
    }

    private DSAction makeShowTableAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest request) {
                String tableName = request.getTargetInfo().getNode().getName();
                request.getParameters().put(STATEMENT, "SELECT * FROM " + tableName);
                DBConnectionNode connNode = (DBConnectionNode) request.getTargetInfo().getParent();
                return connNode.runQuery(request);
            }
        }.setResultsType(ResultsType.TABLE);
        return act;
    }

}
