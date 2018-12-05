package org.iot.dsa.dslink.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.security.DSPasswordAes128;

/**
 * Class designed for handling connections with and arbitrary driver using C3P0 pooling.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
public class JDBCPooledNode extends DBConnectionNode {

    private ComboPooledDataSource pool_data_source = null;

    public JDBCPooledNode() {
    }

    public JDBCPooledNode(DSMap params) {
        super(params);
    }

    @Override
    protected void checkConfig() {
        if (usr_name.getElement().toString().isEmpty()) {
            throw new IllegalStateException("Empty username");
        }
        if (db_url.getElement().toString().isEmpty()) {
            throw new IllegalStateException("Empty url");
        }
        if (driver.getElement().toString().isEmpty()) {
            throw new IllegalStateException("Empty driver");
        }
        configOk();
    }

    @Override
	protected
    void closeConnections() {
        if (pool_data_source != null) {
            pool_data_source.close();
        }
    }

    @Override
	protected
    void createDatabaseConnection() {
        if (!canConnect()) {
            return;
        }
        try {
            String url = db_url.getValue().toString();
            String name = usr_name.getValue().toString();
            String pass = ((DSPasswordAes128) password.getValue()).decode();
            String drvr = driver.getValue().toString();

            pool_data_source = new ComboPooledDataSource();
            pool_data_source.setDriverClass(drvr); //loads the jdbc driver
            pool_data_source.setJdbcUrl(url);
            pool_data_source.setUser(name);
            pool_data_source.setPassword(pass);
            pool_data_source.setAcquireRetryAttempts(6);
            pool_data_source.setAcquireRetryDelay(500);
            pool_data_source.setCheckoutTimeout(3000);
            //TODO: implement dynamic ping cycle/connections
            //pool_data_source.setTestConnectionOnCheckout(true);
            //pool_data_source.setPreferredTestQuery("SELECT 1");

            /*
            //Alternative, uses standard JDBC drivers
            //Might be useful later if implementation that does not need explicit driver passing is desired.
            DataSource ds_unpooled = DataSources.unpooledDataSource(url, name, pass);
            DataSource ds_pooled = DataSources.pooledDataSource( ds_unpooled );
            */
        } catch (PropertyVetoException e) {
            connDown(e.getMessage());
            warn("Failed to connect to Database: " + db_name.getValue() + " Message: " + e);
        }
        testConnection();
    }

    @Override
	protected Connection getConnection() throws SQLException {
        return pool_data_source.getConnection();
    }

    @Override
    DSAction makeEditAction() {
        DSAction act = super.makeEditAction();
        act.addParameter(JDBCv2Helpers.DB_URL, DSValueType.STRING, null)
           .setPlaceHolder("jdbc:mysql://127.0.0.1:3306");
        return act;
    }

}
