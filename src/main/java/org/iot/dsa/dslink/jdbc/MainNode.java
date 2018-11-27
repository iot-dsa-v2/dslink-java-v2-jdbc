package org.iot.dsa.dslink.jdbc;

import org.iot.dsa.dslink.DSMainNode;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;

/**
 * Link main class and root node.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
public class MainNode extends DSMainNode {

    ///////////////////////////////////////////////////////////////////////////
    // Methods
    ///////////////////////////////////////////////////////////////////////////

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault(JDBCv2Helpers.ADD_DB, makeAddDatabaseAction());
//        declareDefault(JDBCv2Helpers.CREATE_DB, makeCreateDatabaseAction());
        declareDefault("Help",
                       DSString.valueOf("https://github.com/iot-dsa-v2/dslink-java-v2-jdbc"))
                .setReadOnly(true).setTransient(true);
        //TODO: Create action to manually add a driver
        //declareDefault(JDBCv2Helpers.ADD_DRIVER, makeAddDriverAction());
    }

    private ActionResult addNewDatabase(DSMap parameters) {
        DSNode nextDB = new C3P0PooledDBConnectionNode(parameters);
        add(parameters.getString(JDBCv2Helpers.DB_NAME), nextDB);
        return null;
    }

    protected DSAction makeAddDatabaseAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo target, ActionInvocation invocation) {
                return ((MainNode) target.get()).addNewDatabase(invocation.getParameters());
            }
        };
        act.addParameter(JDBCv2Helpers.DB_NAME, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_URL, DSValueType.STRING, null)
           .setPlaceHolder("jdbc:mysql://127.0.0.1:3306");
        act.addParameter(JDBCv2Helpers.DB_USER, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_PASSWORD, DSValueType.STRING, null).setEditor("password");
        DSList drivers = JDBCv2Helpers.getRegisteredDrivers();
        act.addParameter(JDBCv2Helpers.DRIVER, DSValueType.ENUM, null).setEnumRange(drivers);
        //TODO: add default timeout/poolable options
        //action.addParameter(new Parameter(JdbcConstants.DEFAULT_TIMEOUT, ValueType.NUMBER));
        //action.addParameter(new Parameter(JdbcConstants.POOLABLE, ValueType.BOOL, new Value(true)));
        return act;
    }

}
