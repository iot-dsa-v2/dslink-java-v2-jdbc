package org.iot.dsa.dslink.java.v2.jdbc;

import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSMap;
import org.iot.dsa.node.DSMetadata;
import org.iot.dsa.node.DSValueType;
import org.iot.dsa.node.action.ActionInvocation;
import org.iot.dsa.node.action.ActionSpec;
import org.iot.dsa.node.action.ActionTable;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Class for handling streaming tables. Currently not used.
 *
 * @author James (Juris) Puchin
 * Created on 10/13/2017
 */
public class JDBCOpenTable implements ActionTable {

    private ActionSpec act;
    private List<DSMap> cols;
    private int columnCount;

    JDBCOpenTable(ActionSpec act, ResultSet res, Logger loggern) throws SQLException {
        this.act = act;

        ResultSetMetaData meta = res.getMetaData();
        columnCount = meta.getColumnCount();
        cols = new LinkedList<>();
        for (int i = 1; i <= columnCount; i++) {
            cols.add(makeStrColumn(meta.getColumnName(i)));
        }
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
        return new LinkedList<DSList>().iterator();
    }

    public void sendRows(ResultSet res, ActionInvocation invoc) throws SQLException {
        //noinspection StatementWithEmptyBody
        if (res.getMetaData().getColumnCount() == columnCount) {
            while (res.next()) {
                DSList row = new DSList();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(res.getString(i));
                }
                invoc.send(row);
            }
        } else {
            //TODO: clean up error handling, is exception needed here?
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
