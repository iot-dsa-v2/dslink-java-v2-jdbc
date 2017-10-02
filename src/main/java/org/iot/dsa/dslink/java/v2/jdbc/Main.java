package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.DSRuntime;
import org.iot.dsa.dslink.DSRootNode;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSFlexEnum;
import org.iot.dsa.node.DSInfo;
import org.iot.dsa.node.DSInt;
import org.iot.dsa.node.DSJavaEnum;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionResult;
import org.iot.dsa.node.action.DSAction;

/**
 * Link main class and root node.
 *
 * @author Aaron Hansen
 */
public class Main extends DSRootNode implements Runnable {

    ///////////////////////////////////////////////////////////////////////////
    // Constants
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Fields
    ///////////////////////////////////////////////////////////////////////////

    private DSInfo incrementingInt = getInfo("Incrementing Int");
    private DSInfo reset = getInfo("Reset");
    private DSInfo test = getInfo("Test");
    private DSRuntime.Timer timer;

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Methods
    ///////////////////////////////////////////////////////////////////////////

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault("Incrementing Int", DSInt.valueOf(1)).setReadOnly(true);
        declareDefault("Writable Boolean", DSBool.valueOf(true)).setConfig(true);
        declareDefault("Writable Enum",
                       DSFlexEnum.valueOf("On",
                                          DSList.valueOf("Off", "On", "Auto", "Has Space")));
        declareDefault("Java Enum", DSJavaEnum.valueOf(MyEnum.Off));
        declareDefault("Message", DSString.EMPTY).setReadOnly(true);
        DSAction action = new DSAction();
        action.addParameter("Arg",
                            DSJavaEnum.valueOf(MyEnum.Off),
                            "My action description");
        declareDefault("Reset", action);
        declareDefault("Test", new DSAction());
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
        } else if (actionInfo == this.test) {
            DSRuntime.run(new Runnable() {
                @Override
                public void run() {
                    onTest();
                }
            });
            return null;
        }
        return super.onInvoke(actionInfo, invocation);
    }

    /**
     * Start the update timer.  This only updates when something is interested in this node.
     */
    @Override
    protected synchronized void onSubscribed() {
        timer = DSRuntime.run(this, System.currentTimeMillis() + 1000l, 1000l);
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
     * Build a large database
     */
    private void onTest() {
        long start = System.currentTimeMillis();
        clear();
        long now = System.currentTimeMillis();
        long dur = now - start;
        info("********** Clear duration: " + dur);
        start = System.currentTimeMillis();
        DSNode node = new TestNode();
        add("test", node).setConfig(true);
        for (int i = 0; i < 10; i++) {
            DSNode iNode = new TestNode();
            node.add("test" + i, iNode);
            for (int j = 0; j < 100; j++) {
                DSNode jNode = new TestNode();
                iNode.add("test" + j, jNode);
                for (int k = 0; k < 100; k++) {
                    jNode.add("test" + k, new TestNode());
                }
            }
        }
        now = System.currentTimeMillis();
        dur = now - start;
        info("********** Add duration: " + dur);
        start = System.currentTimeMillis();
        getLink().save();
        now = System.currentTimeMillis();
        dur = now - start;
        info("********** Save duration: " + dur);
        start = System.currentTimeMillis();
        onTestIterate(node);
        now = System.currentTimeMillis();
        dur = now - start;
        info("********** Iterate duration: " + dur);
    }

    /**
     * Walk the test tree.
     */
    private void onTestIterate(DSNode node) {
        for (DSInfo info : node) {
            if (info.isNode()) {
                onTestIterate(info.getNode());
            }
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

    public static class TestNode extends DSNode {

        @Override
        protected void declareDefaults() {
            /*
            super.declareDefaults();
            declareDefault("Incrementing Int", DSInt.valueOf(1)).setReadOnly(true);
            declareDefault("Writable Boolean", DSBool.valueOf(true));
            declareDefault("Writable Enum",
                           DSFlexEnum.valueOf("On",
                                              DSList.valueOf("Off", "On", "Auto", "Has Space")));
            declareDefault("Java Enum", DSJavaEnum.valueOf(MyEnum.Off));
            declareDefault("Message", DSString.EMPTY).setReadOnly(true);
            DSAction action = new DSAction();
            action.addParameter("Arg",
                                DSJavaEnum.valueOf(MyEnum.Off),
                                "My action description");
            declareDefault("Reset", action);
            */
        }

        @Override
        protected void onStable() {
            put("Number", DSInt.valueOf(1));
            DSAction action = new DSAction();
            action.addParameter("Arg1", DSString.valueOf("ID1"), null);
            action.addParameter("Arg2", DSString.valueOf("ID2"), null);
            action.addParameter("Arg3", DSString.valueOf("ID3"), null);
            put("action1", action);
            /*
            action = new DSAction();
            action.addParameter("Arg1", DSString.valueOf("ID1"), null);
            action.addParameter("Arg2", DSString.valueOf("ID2"), null);
            action.addParameter("Arg3", DSString.valueOf("ID3"), null);
            */
            put("action2", action);
            /*
            action = new DSAction();
            action.addParameter("Arg1", DSString.valueOf("ID1"), null);
            action.addParameter("Arg2", DSString.valueOf("ID2"), null);
            action.addParameter("Arg3", DSString.valueOf("ID3"), null);
            */
            put("action3", action);
        }

    }

    public enum MyEnum {
        On,
        Off,
        Auto
    }

    ///////////////////////////////////////////////////////////////////////////
    // Initialization
    ///////////////////////////////////////////////////////////////////////////

}
