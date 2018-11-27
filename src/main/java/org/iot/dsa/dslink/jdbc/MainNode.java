package org.iot.dsa.dslink.jdbc;

import org.iot.dsa.dslink.DSMainNode;
import org.iot.dsa.node.DSElement;
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
        declareDefault(JDBCv2Helpers.CREATE_DB, makeCreateDatabaseAction());
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

    private ActionResult addNewDriver(DSMap parameters) {
        String drvr = parameters.getString(JDBCv2Helpers.DRIVER_NAME);
        registerDriver(drvr);
        return null;
    }

    private ActionResult createNewDatabase(DSMap parameters) {
        parameters.put(JDBCv2Helpers.DRIVER, DSElement.make("org.h2.Driver"));
        parameters.put(JDBCv2Helpers.DB_URL, DSElement.make("Not Started"));
        DSNode nextDB = new ManagedH2DBConnectionNode(parameters);
        add(parameters.getString(JDBCv2Helpers.DB_NAME), nextDB);
        return null;
    }

    private DSAction makeAddDatabaseAction() {
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

    private DSAction makeAddDriverAction() {
        //TODO: Implement the addition of user-defined drivers
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo target, ActionInvocation invocation) {
                return ((MainNode) target.get()).addNewDriver(invocation.getParameters());
            }
        };
        act.addParameter(JDBCv2Helpers.DRIVER_NAME, DSValueType.STRING, null);
        DSList drivers = JDBCv2Helpers.getRegisteredDrivers();
        act.addParameter(JDBCv2Helpers.REGISTERED, DSValueType.ENUM, null).setEnumRange(drivers);
        return act;
    }

    private DSAction makeCreateDatabaseAction() {
        DSAction act = new DSAction.Parameterless() {
            @Override
            public ActionResult invoke(DSInfo target, ActionInvocation invocation) {
                return ((MainNode) target.get()).createNewDatabase(invocation.getParameters());
            }
        };
        act.addParameter(JDBCv2Helpers.DB_NAME, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_USER, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_PASSWORD, DSValueType.STRING, null).setEditor("password");
        //TODO: add default timeout/poolable options
        //action.addParameter(new Parameter(JdbcConstants.DEFAULT_TIMEOUT, ValueType.NUMBER));
        //action.addParameter(new Parameter(JdbcConstants.POOLABLE, ValueType.BOOL, new Value(true)));
        return act;
    }

    private void registerDriver(String drvr) {
        try {
            JDBCv2Helpers.registerDriver(drvr);
        } catch (ClassNotFoundException e) {
            warn("Driver class not found: " + drvr, e);
        }
    }

}
