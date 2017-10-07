package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSMetadata;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.ActionTable;
import org.iot.dsa.util.DSException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class JDBCOpenTable implements ActionTable {

    private ActionSpec act;
    private List<DSMap> cols;
    private List<DSList> rows;
    private int columnCount;

    JDBCOpenTable(ActionSpec act, ResultSet res) throws SQLException {
        this.act = act;

        ResultSetMetaData meta = res.getMetaData();
        columnCount = meta.getColumnCount();
        cols = new LinkedList<>();
        for (int i = 1; i <= columnCount; i++) {
            cols.add(makeStrColumn(meta.getColumnName(i)));
        }

        rows = new LinkedList<>();
    }

    private static DSMap makeStrColumn(String name) {
        return new DSMetadata().setName(name).setType(DSValueType.STRING).getMap();
    }

    @Override
    public Iterator<DSMap> getColumns() {
        return cols.iterator();
    }

    @Override
    public Iterator<DSList> getRows() {
        return rows.iterator();
    }

    public void addRows(ResultSet res) throws SQLException {
        if (res.getMetaData().getColumnCount() == columnCount) {
            while (res.next()) {
                DSList row = new DSList();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(res.getString(i));
                }
                rows.add(row);
            }
        } else {
            //TODO: clean up error handling
        }
    }

    @Override
    public ActionSpec getAction() {
        return act;
    }

    @Override
    public void onClose() {

    }
}
