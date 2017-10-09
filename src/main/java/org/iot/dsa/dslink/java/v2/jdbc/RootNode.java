package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.dslink.DSRootNode;
import org.iot.dsa.node.*;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;

/**
 * Link main class and root node.
 *
 * @author Aaron Hansen
 */
public class RootNode extends DSRootNode {

    ///////////////////////////////////////////////////////////////////////////
    // Constants
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Fields
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Methods
    ///////////////////////////////////////////////////////////////////////////

    private void registerDriver(String drvr) {
        try {
            JDBCv2Helpers.registerDriver(drvr);
        } catch (ClassNotFoundException e) {
            warn("Driver class not found: " + drvr);
            warn(e);
        }
    }

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault(JDBCv2Helpers.ADD_DB, makeAddDatabaseAction());
        //TODO: Create action to manually add a driver
        //declareDefault(JDBCv2Helpers.ADD_DRIVER, makeAddDriverAction());
    }

    private DSAction makeAddDatabaseAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((RootNode) info.getParent()).addNewDatabase(invocation.getParameters());
            }
        };
        act.addParameter(JDBCv2Helpers.DB_NAME, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_URL, DSValueType.STRING, null).setPlaceHolder("jdbc:mysql://127.0.0.1:3306");
        act.addParameter(JDBCv2Helpers.DB_USER, DSValueType.STRING, null);
        act.addParameter(JDBCv2Helpers.DB_PASSWORD, DSValueType.STRING, null).setEditor("password");
        DSList drivers = JDBCv2Helpers.getRegisteredDrivers();
        act.addParameter(JDBCv2Helpers.DRIVER, DSValueType.ENUM, null).setEnumRange(drivers);
        //TODO: add default timeout/poolable options
        //action.addParameter(new Parameter(JdbcConstants.DEFAULT_TIMEOUT, ValueType.NUMBER));
        //action.addParameter(new Parameter(JdbcConstants.POOLABLE, ValueType.BOOL, new Value(true)));
        return act;
    }

    private ActionResult addNewDatabase(DSMap parameters) {
        DSNode nextDB = new DBConnectionNode(parameters);
        add(parameters.getString(JDBCv2Helpers.DB_NAME), nextDB);
        getLink().save();
        return null;
    }

    private DSAction makeAddDriverAction() {
        //TODO: Implement the addition of user-defined drivers
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((RootNode) info.getParent()).addNewDriver(invocation.getParameters());
            }
        };
        act.addParameter(JDBCv2Helpers.DRIVER_NAME, DSValueType.STRING, null);
        DSList drivers = JDBCv2Helpers.getRegisteredDrivers();
        act.addParameter(JDBCv2Helpers.REGISTERED, DSValueType.ENUM, null).setEnumRange(drivers);
        return act;
    }

    private ActionResult addNewDriver(DSMap parameters) {
        String drvr = parameters.getString(JDBCv2Helpers.DRIVER_NAME);
        registerDriver(drvr);
        return null;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Inner Classes
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Initialization
    ///////////////////////////////////////////////////////////////////////////

}
