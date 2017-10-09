package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.*;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.ActionTable;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static java.awt.image.DataBuffer.TYPE_DOUBLE;

public class JDBCClosedTable implements ActionTable {

    private ActionSpec act;
    private List<DSMap> cols;
    private List<DSList> rows;

    JDBCClosedTable(ActionSpec act, ResultSet res) throws SQLException {
        this.act = act;

        ResultSetMetaData meta = res.getMetaData();
        int columnCount = meta.getColumnCount();

        cols = new LinkedList<>();
        for (int i = 1; i <= columnCount; i++) {
            DSValueType type = getColumnType(meta, i);
            cols.add(makeColumn(meta.getColumnName(i), type));
        }

        rows = new LinkedList<>();
        while (res.next()) {
            DSList row = new DSList();
            for (int i = 1; i <= columnCount; i++) {
                row.add(res.getString(i));
            }
            rows.add(row);
        }
    }

    private static DSMap makeColumn(String name, DSValueType type) {
        return new DSMetadata().setName(name).setType(type).getMap();
    }

    private static DSValueType getColumnType(ResultSetMetaData meta, int idx) {
        DSValueType ret = DSValueType.STRING;

        try {
            switch (meta.getColumnType(idx))
            {
                //null
                case Types.NULL :
                    ret = DSValueType.STRING;
                    break;
                //boolean
                case Types.BOOLEAN :
                    ret = DSValueType.BOOL;
                    break;
                //date
                case Types.DATE :
                case Types.TIMESTAMP :
                    ret = DSValueType.STRING;
                    break;
                //double
                case Types.DECIMAL :
                case Types.DOUBLE :
                case Types.FLOAT :
                case Types.NUMERIC :
                case Types.REAL :
                    ret = DSValueType.NUMBER;
                    break;
                //duration
                case Types.TIME :
                    ret = DSValueType.STRING;
                    break;
                //long
                case Types.BIT :
                case Types.BIGINT :
                case Types.INTEGER :
                case Types.SMALLINT :
                case Types.TINYINT :
                    ret = DSValueType.NUMBER;
                    break;
                // String by default
            }
        } catch (SQLException e) {
            //Not critical, will default to string in case of error.
        }

        return ret;
    }

    @Override
    public Iterator<DSMap> getColumns() {
        return cols.iterator();
    }

    @Override
    public Iterator<DSList> getRows() {
        return rows.iterator();
    }

    @Override
    public ActionSpec getAction() {
        return act;
    }

    @Override
    public void onClose() {

    }
}
