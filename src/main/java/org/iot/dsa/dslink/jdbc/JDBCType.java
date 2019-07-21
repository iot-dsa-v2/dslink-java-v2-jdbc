package org.iot.dsa.dslink.jdbc;

import java.util.HashMap;
import java.util.Map;
import org.iot.dsa.node.DSElement;
import org.iot.dsa.node.DSIEnum;
import org.iot.dsa.node.DSIObject;
import org.iot.dsa.node.DSIValue;
import org.iot.dsa.node.DSList;
import org.iot.dsa.node.DSRegistry;
import org.iot.dsa.node.DSString;
import org.iot.dsa.node.DSValueType;

/**
 * Enum representing various types of rollup functions.
 *
 * @author Aaron Hansen
 */
public enum JDBCType implements DSIEnum, DSIValue {

    ///////////////////////////////////////////////////////////////////////////
    // Class Fields
    ///////////////////////////////////////////////////////////////////////////

    STRING("String"),
    BOOLEAN("Boolean"),
    DOUBLE("Double"),
    DECIMAL("Decimal"),
    FLOAT("Float"),
    INTEGER("Integer"),
    NUMERIC("Numeric");

    private static final Map<String, JDBCType> enums = new HashMap<>();

    ///////////////////////////////////////////////////////////////////////////
    // Instance Fields
    ///////////////////////////////////////////////////////////////////////////

    private DSString element;

    ///////////////////////////////////////////////////////////////////////////
    // Constructors
    ///////////////////////////////////////////////////////////////////////////

    private JDBCType(String display) {
        this.element = DSString.valueOf(display);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Public Methods
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public DSIObject copy() {
        return this;
    }

    @Override
    public DSList getEnums(DSList bucket) {
        if (bucket == null) {
            bucket = new DSList();
        }
        for (JDBCType e : values()) {
            bucket.add(e.toElement());
        }
        return bucket;
    }

    @Override
    public DSValueType getValueType() {
        return DSValueType.ENUM;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public DSElement toElement() {
        return element;
    }

    @Override
    public String toString() {
        return element.toString();
    }

    /**
     * Get an instance from a string.
     */
    public static JDBCType valueFor(String display) {
        JDBCType ret = enums.get(display);
        if (ret == null) {
            ret = enums.get(display.toLowerCase());
        }
        return ret;
    }

    @Override
    public DSIValue valueOf(DSElement element) {
        return valueFor(element.toString());
    }

    /////////////////////////////////////////////////////////////////
    // Initialization
    /////////////////////////////////////////////////////////////////

    static {
        DSRegistry.registerDecoder(JDBCType.class, STRING);
        for (JDBCType e : STRING.values()) {
            enums.put(e.name(), e);
            enums.put(e.toString(), e);
            enums.put(e.toString().toLowerCase(), e);
        }
    }

}//JDBCType
