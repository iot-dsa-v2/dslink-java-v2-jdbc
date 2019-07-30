package org.iot.dsa.dslink.jdbc;

import org.iot.dsa.dslink.DSMainNode;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.action.DSAction;

/**
 * Link main class and root node.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
public abstract class AbstractMainNode extends DSMainNode implements JDBCObject {

    public static final String CONNECT = "Connect";
    public static final String CONNECTION = "Connection";

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault(CONNECT, makeNewConnectionAction())
                .getMetadata().setActionGroup(DSAction.NEW_GROUP, CONNECTION);
        declareDefault("Help", DSString.valueOf(getHelpUrl()))
                .setReadOnly(true).setTransient(true);
    }

    protected String getHelpUrl() {
        return "https://github.com/iot-dsa-v2/dslink-java-v2-jdbc";
    }

    abstract protected DSAction makeNewConnectionAction();

}
