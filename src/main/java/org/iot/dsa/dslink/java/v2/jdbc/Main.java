package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.DSRuntime;
import org.iot.dsa.dslink.DSRequester;
import org.iot.dsa.dslink.DSRequesterInterface;
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
public class Main extends DSRootNode implements Runnable, DSRequester {

    ///////////////////////////////////////////////////////////////////////////
    // Constants
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Fields
    ///////////////////////////////////////////////////////////////////////////

    private DSInfo incrementingInt = getInfo("Incrementing Int");
    private DSInfo reset = getInfo("Reset");
    private DSInfo addDB = getInfo(JDBCv2Helpers.ADD_DB);
    private DSRuntime.Timer timer;
    private static DSRequesterInterface session;

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Methods
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public void onConnected(DSRequesterInterface session) {
        Main.session = session;
    }

    @Override
    public void onDisconnected(DSRequesterInterface session) {
    }

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault("Incrementing Int", DSInt.valueOf(1)).setReadOnly(true);
        DSAction action = new DSAction();
        declareDefault("Reset", action);
        declareDefault(JDBCv2Helpers.ADD_DB, makeAddDatabaseAction());
    }

    private DSAction makeAddDatabaseAction() {
        DSAction act = new DSAction() {
            @Override
            public ActionResult invoke(DSInfo info, ActionInvocation invocation) {
                return ((Main) info.getParent()).addNewDatabase(invocation.getParameters());
            }
        };
        act.addParameter(JDBCv2Helpers.DB_NAME, DSValueType.STRING, null);
        return act;
    }

    private ActionResult addNewDatabase(DSMap parameters) {
        DSNode nextDB = new DBConnectionNode(parameters);
        add(parameters.getString(JDBCv2Helpers.DB_NAME), nextDB);
        getLink().save();
        return null;
    }

    /**
     * Handles the reset action.
     */
    @Override
    public ActionResult onInvoke(DSInfo actionInfo, ActionInvocation invocation) {
        if (actionInfo == this.reset) {
            put(incrementingInt, DSInt.valueOf(0));
            DSElement arg = invocation.getParameters().get("Arg");
            put("Message", arg);
            clear();
            return null;
        }
        return super.onInvoke(actionInfo, invocation);
    }

    /**
     * Start the update timer.  This only updates when something is interested in this node.
     */
    @Override
    protected synchronized void onSubscribed() {
        timer = DSRuntime.run(this, System.currentTimeMillis() + 1000, 1000);
    }

    /**
     * Cancel an active timer if there is one.
     */
    @Override
    protected void onStopped() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }

    /**
     * Cancel the active timer.
     */
    @Override
    protected synchronized void onUnsubscribed() {
        timer.cancel();
        timer = null;
    }

    /**
     * Called by an internal timer, increments an integer child on a one second interval, only when
     * this node is subscribed.
     */
    @Override
    public void run() {
        DSInt value = (DSInt) incrementingInt.getValue();
        put(incrementingInt, DSInt.valueOf(value.toInt() + 1));
    }

    ///////////////////////////////////////////////////////////////////////////
    // Inner Classes
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Initialization
    ///////////////////////////////////////////////////////////////////////////

}
