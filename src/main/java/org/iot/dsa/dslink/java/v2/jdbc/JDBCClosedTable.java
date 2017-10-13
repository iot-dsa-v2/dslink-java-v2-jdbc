package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.*;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.ActionTable;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Class designed to handle tables for non-streaming queries.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
public class JDBCClosedTable implements ActionTable {

    private ActionSpec act;
    private ResultSet res;
    private Logger log;
    private List<DSMap> cols;
    private int columnCount;
    private ColType[] colTypes;

    JDBCClosedTable(ActionSpec act, ResultSet res, Logger log) throws SQLException {
        this.act = act;
        this.res = res;
        this.log = log;

        ResultSetMetaData meta = res.getMetaData();
        columnCount = meta.getColumnCount();

        colTypes = new ColType[columnCount + 1];
        cols = new LinkedList<>();
        for (int i = 1; i <= columnCount; i++) {
            DSValueType type = setColumnType(meta, i);
            cols.add(makeColumn(meta.getColumnName(i), type));
        }
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

    private DSList getCurrentRow() throws SQLException {
        DSList row = new DSList();
        for (int idx = 1; idx <= columnCount; idx++) {
            try {
                switch (colTypes[idx]) {
                    case TYPE_BOOLEAN:
                        row.add(res.getBoolean(idx));
                        continue;
                    case TYPE_DATE:
                        java.sql.Timestamp d = res.getTimestamp(idx);
                        if (d == null)
                            row.add(DSElement.makeNull());
                        else
                            row.add(org.iot.dsa.time.DSDateTime.valueOf(d.getTime()).toString());
                        continue;
                    case TYPE_TIME:
                        java.sql.Time t = res.getTime(idx);
                        if (t == null)
                            row.add(DSElement.makeNull());
                        else
                            row.add(t.toString());
                        continue;
                    case TYPE_DOUBLE:
                        row.add(res.getDouble(idx));
                        continue;
                    case TYPE_LONG:
                        row.add(res.getLong(idx));
                        continue;
                    case TYPE_NULL:
                    case TYPE_IGNORE:
                        row.add(DSElement.makeNull());
                        continue;
                }
                String str = res.getString(idx);
                if (str == null)
                    row.add(DSElement.makeNull());
                else
                    row.add(str);
                continue;
            } catch (Exception x) {
                log.warning("Failed to get row element: " + idx + " error: " + x);
            }
            row.add(DSElement.makeNull());
        }
        return row;
    }

    @Override
    public Iterator<DSMap> getColumns() {
        return cols.iterator();
    }

    @Override
    public Iterator<DSList> getRows() {
        try {
            if (!res.next()) {
                //Empty result
                return new ArrayList<DSList>().iterator();
            }
        } catch (SQLException e) {
            log.warning("Table is empty! " + e);
        }
        return new Iterator<DSList>() {

            @Override
            public boolean hasNext() {
                //noinspection UnusedAssignment
                boolean result = true;
                try {
                    result = !res.isAfterLast();
                } catch (SQLException e) {
                    //No next if broken :)
                    result = false;
                }
                return result;
            }

            @Override
            public DSList next() {
                DSList row = null;
                try {
                    if (res.isBeforeFirst()) res.next();
                    row = getCurrentRow();
                    res.next();
                } catch (SQLException e) {
                    log.warning("Failed to fetch next row: " + e);
                }
                return row;
            }

            @Override
            public void remove() {
                //Does nothing
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public ActionSpec getAction() {
        return act;
    }

    @Override
    public void onClose() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = res.getStatement().getConnection();
            stmt = res.getStatement();
        } catch (SQLException e) {
            log.warning("Failed to properly close the connection: " + e);
        }
        JDBCv2Helpers.cleanClose(res, stmt, conn, log);
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
