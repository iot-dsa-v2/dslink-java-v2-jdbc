package org.iot.dsa.dslink.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import org.iot.dsa.node.DSBool;
import org.iot.dsa.node.DSDouble;
import org.iot.dsa.node.DSIValue;
import org.iot.dsa.node.DSLong;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSMetadata;
import org.iot.dsa.node.DSNode;
import org.iot.dsa.node.DSNull;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.ActionTable;
import org.iot.dsa.util.DSException;

/**
 * Class designed to handle tables for non-streaming queries.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
public class JDBCClosedTable implements ActionTable {

    private ActionSpec act;
    private ColType[] colTypes;
    private List<DSMap> cols;
    private int columnCount;
    private DSNode node;
    private ResultSet res;

    JDBCClosedTable(ActionSpec act, ResultSet res, DSNode node) throws SQLException {
        this.act = act;
        this.res = res;
        this.node = node;
        ResultSetMetaData meta = res.getMetaData();
        columnCount = meta.getColumnCount();
        colTypes = new ColType[columnCount + 1];
        cols = new LinkedList<DSMap>();
        for (int i = 1; i <= columnCount; i++) {
            DSValueType type = setColumnType(meta, i);
            cols.add(makeColumn(meta.getColumnName(i), type));
        }
    }

    @Override
    public ActionSpec getAction() {
        return act;
    }

    @Override
    public int getColumnCount() {
        return cols.size();
    }

    @Override
    public void getMetadata(int col, DSMap bucket) {
        bucket.putAll(cols.get(col));
    }

    @Override
    public DSIValue getValue(int col) {
        try {
            switch (colTypes[col]) {
                case TYPE_BOOLEAN:
                    return DSBool.valueOf(res.getBoolean(col));
                case TYPE_DATE:
                    java.sql.Timestamp d = res.getTimestamp(col);
                    if (d == null) {
                        return DSNull.NULL;
                    }
                    return org.iot.dsa.time.DSDateTime.valueOf(d.getTime());
                case TYPE_TIME:
                    java.sql.Time t = res.getTime(col);
                    if (t == null) {
                        return DSNull.NULL;
                    }
                    return org.iot.dsa.time.DSDateTime.valueOf(t.getTime());
                case TYPE_DOUBLE:
                    return DSDouble.valueOf(res.getDouble(col));
                case TYPE_LONG:
                    return DSLong.valueOf(res.getLong(col));
                case TYPE_NULL:
                case TYPE_IGNORE:
                    return DSNull.NULL;
            }
            String str = res.getString(col);
            if (str == null) {
                return DSNull.NULL;
            }
            return DSString.valueOf(str);
        } catch (Exception x) {
            DSException.throwRuntime(x);
        }
        return DSNull.NULL;
    }

    @Override
    public boolean next() {
        try {
            return res.next();
        } catch (SQLException x) {
            DSException.throwRuntime(x);
        }
        return false;
    }

    @Override
    public void onClose() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = res.getStatement().getConnection();
            stmt = res.getStatement();
        } catch (SQLException e) {
            node.warn("Failed to properly close the connection: " + e);
        }
        JDBCv2Helpers.cleanClose(res, stmt, conn, node);
    }

    private static DSMap makeColumn(String name, DSValueType type) {
        return new DSMetadata().setName(name).setType(type).getMap();
    }

    private DSValueType setColumnType(ResultSetMetaData meta, int idx) {
        DSValueType ret = DSValueType.STRING;
        colTypes[idx] = ColType.TYPE_STRING;
        try {
            switch (meta.getColumnType(idx)) {
                //null
                case Types.NULL:
                    ret = DSValueType.STRING;
                    colTypes[idx] = ColType.TYPE_STRING;
                    break;
                //boolean
                case Types.BOOLEAN:
                    ret = DSValueType.BOOL;
                    colTypes[idx] = ColType.TYPE_BOOLEAN;
                    break;
                //date
                case Types.DATE:
                case Types.TIMESTAMP:
                    ret = DSValueType.STRING;
                    colTypes[idx] = ColType.TYPE_DATE;
                    break;
                //double
                case Types.DECIMAL:
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.NUMERIC:
                case Types.REAL:
                    ret = DSValueType.NUMBER;
                    colTypes[idx] = ColType.TYPE_DOUBLE;
                    break;
                //duration
                case Types.TIME:
                    ret = DSValueType.STRING;
                    colTypes[idx] = ColType.TYPE_TIME;
                    break;
                //long
                case Types.BIT:
                case Types.BIGINT:
                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.TINYINT:
                    ret = DSValueType.NUMBER;
                    colTypes[idx] = ColType.TYPE_LONG;
                    break;
            }
        } catch (SQLException e) {
            //Not critical, will default to string in case of error.
        }
        return ret;
    }

    private enum ColType {
        TYPE_BOOLEAN,
        TYPE_DATE,
        TYPE_DOUBLE,
        TYPE_IGNORE,
        TYPE_LONG,
        TYPE_NULL,
        TYPE_STRING,
        TYPE_TIME
    }
}
