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
public abstract class AbstractMainNode extends DSMainNode {

    ///////////////////////////////////////////////////////////////////////////
    // Methods
    ///////////////////////////////////////////////////////////////////////////

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault(JDBCv2Helpers.ADD_DB, makeAddDatabaseAction());
        declareDefault("Help",
                       DSString.valueOf(getHelpUrl()))
                .setReadOnly(true).setTransient(true);
        //TODO: Create action to manually add a driver
        //declareDefault(JDBCv2Helpers.ADD_DRIVER, makeAddDriverAction());
    }

    protected String getHelpUrl() {
        return "https://github.com/iot-dsa-v2/dslink-java-v2-jdbc";
    }

    abstract protected DSAction makeAddDatabaseAction();

}
