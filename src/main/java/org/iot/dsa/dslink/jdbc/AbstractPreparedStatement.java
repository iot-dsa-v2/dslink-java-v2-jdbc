package org.iot.dsa.dslink.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.iot.dsa.dslink.ActionResults;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSLong;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSMetadata;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.DSAction;
import org.iot.dsa.node.action.DSIActionRequest;

/**
 * Represents a prepared statement that can be used for queries.
 *
 * addParameter
 *
 * @author Aaron Hansen
 */
public abstract class AbstractPreparedStatement extends DSNode implements JDBCObject {

    ///////////////////////////////////////////////////////////////////////////
    // Class Fields
    ///////////////////////////////////////////////////////////////////////////

    static final String EXECUTE = "Execute";
    static final String PARAMETER = "Parameter";
    static final String PARAMETERS = "Parameters";
    static final String COUNT = "Count";
    static final String SET_PARAM_COUNT = "Set Parameter Count";

    ///////////////////////////////////////////////////////////////////////////
    // Instance Fields
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Public Methods
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Protected Methods
    ///////////////////////////////////////////////////////////////////////////

    @Override
    protected void declareDefaults() {
        super.declareDefaults();
        declareDefault(STATEMENT, DSString.EMPTY, "Statement to execute");
        declareDefault(PARAMETERS, DSLong.valueOf(0), "Number of parameters in the query")
                .setPrivate(true);
        declareDefault(EXECUTE, DSAction.DEFAULT, "Execute the statement")
                .setTransient(true);
        declareDefault(SET_PARAM_COUNT, new DSAction() {
            @Override
            public ActionResults invoke(DSIActionRequest request) {
                int count = request.getParameters().getInt(COUNT);
                ((AbstractPreparedStatement) request.getTarget()).setParameterCount(count);
                return null;
            }

            {
                addParameter(COUNT, DSLong.valueOf(0), "Number of parameters");
            }
        }, "Reset the parameter of the query");
    }

    protected DBConnectionNode getConn() {
        return (DBConnectionNode) getParent();
    }

    protected abstract ActionResults getResults(Connection conn,
                                                PreparedStatement stmt,
                                                DSIActionRequest request);

    @Override
    protected void onStarted() {
        super.onStarted();
        put(EXECUTE, new MyQueryAction());
    }

    protected void setParameterCount(int count) {
        int old = getElement(PARAMETERS).toInt();
        if (count < old) {
            for (int i = count + 1; i <= old; i++) {
                remove(PARAMETER + i + "-Name");
                remove(PARAMETER + i + "-Type");
            }
        } else if (count > old) {
            for (int i = old + 1; i <= count; i++) {
                put(PARAMETER + i + "-Name", PARAMETER + i);
                put(PARAMETER + i + "-Type", JDBCType.STRING);
            }
        }
        put(PARAMETERS, DSLong.valueOf(count));
    }

    protected void setStatement(String statement) {
        put(STATEMENT, statement);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Package Methods
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Private Methods
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Inner Classes
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    // Initialization
    ///////////////////////////////////////////////////////////////////////////

    private class MyQueryAction extends DSAction {

        @Override
        public int getParameterCount() {
            return getElement(PARAMETERS).toInt();
        }

        @Override
        public void getParameterMetadata(int idx, DSMap bucket) {
            idx++;
            String name = getElement(PARAMETER + idx + "-Name").toString();
            bucket.put(DSMetadata.NAME, name);
            String type = getElement(PARAMETER + idx + "-Type").toString();
            JDBCType jdbcType = JDBCType.valueFor(type);
            DSValueType valueType = null;
            switch (jdbcType) {
                case BOOLEAN:
                    valueType = DSValueType.BOOL;
                    break;
                case DOUBLE:
                case DECIMAL:
                case FLOAT:
                case NUMERIC:
                    valueType = DSValueType.NUMBER;
                    break;
                case INTEGER:
                    valueType = DSValueType.NUMBER;
                    bucket.put(DSMetadata.PRECISION, 0);
                    break;
                default: //case STRING:
                    valueType = DSValueType.STRING;
                    break;
            }
            bucket.put(DSMetadata.TYPE, valueType.toString());
        }

        @Override
        public ActionResults invoke(DSIActionRequest request) {
            Connection conn = null;
            PreparedStatement stmt = null;
            ResultSet res = null;
            try {
                conn = getConn().getConnection();
                stmt = conn.prepareStatement(getElement(STATEMENT).toString());
            } catch (SQLException x) {
                error(x);
                getConn().connDown(x.getMessage());
                cleanClose(res, stmt, conn, AbstractPreparedStatement.this);
                throw new IllegalStateException("Database connection failed: " + x);
            }
            try {
                int count = getElement(PARAMETERS).toInt();
                DSMap params = request.getParameters();
                for (int i = 1; i <= count; i++) {
                    String name = get(PARAMETER + i + "-Name").toString();
                    DSElement value = params.get(name);
                    if (value == null) {
                        throw new IllegalArgumentException("Missing " + name);
                    }
                    String type = get(PARAMETER + i + "-Type").toString();
                    switch (JDBCType.valueFor(type)) {
                        case BOOLEAN:
                            stmt.setBoolean(i, value.toBoolean());
                            break;
                        case DOUBLE:
                        case DECIMAL:
                        case NUMERIC:
                            stmt.setDouble(i, value.toDouble());
                            break;
                        case FLOAT:
                            stmt.setFloat(i, value.toFloat());
                            break;
                        case INTEGER:
                            stmt.setInt(i, value.toInt());
                            break;
                        default: //case STRING:
                            stmt.setString(i, value.toString());
                            break;
                    }
                }
            } catch (SQLException x) {
                error(x);
                throw new IllegalArgumentException("Query failed: " + x);
            }
            return getResults(conn, stmt, request);
        }

        {
            if (AbstractPreparedStatement.this instanceof JDBCPreparedQuery) {
                setResultsType(ResultsType.TABLE);
            }
        }

    }

}
