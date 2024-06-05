package org.liu.util;


import org.liu.constant.Constant;

public class Utils {
    public static String getTablePath(String layer, String tableName) {
        return String.format("%s/%s/%s", Constant.WAREHOUSE_DIR, layer, tableName);
    }

    public static String getTableName(String layer, String tableName) {
        return Constant.DELTA_DB + "." + layer + "_" + tableName;
    }

    public static String getTableCheckpointPath(String layer, String tableName) {
        return String.format("%s/%s/%s/%s/%s", Constant.WAREHOUSE_DIR, Constant.DELTA_DB, "checkpoints", layer, tableName);
    }
}
