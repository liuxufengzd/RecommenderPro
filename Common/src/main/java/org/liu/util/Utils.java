package org.liu.util;


import org.liu.constant.Constant;

public class Utils {
    public static String getTablePath(String layer, String tableName) {
        return String.format("%s/%s/%s", Constant.WAREHOUSE_BATCH_DIR, layer, tableName);
    }

    public static String getTableCheckpointPath(String layer, String tableName) {
        return String.format("%s/%s/%s/%s", Constant.WAREHOUSE_STREAM_DIR, "checkpoints", layer, tableName);
    }

    public static String getTableName(String layer, String tableName) {
        return Constant.DELTA_DB + "." + layer + "_" + tableName;
    }

    public static String getLayer(String simpleLayerName) {
        var layer = simpleLayerName;
        switch (layer.toLowerCase()) {
            case "ods":
                layer = Constant.LAYER_ODS;
                break;
            case "dwd":
                layer = Constant.LAYER_DWD;
                break;
            case "dws":
                layer = Constant.LAYER_DWS;
                break;
            default:
                layer = Constant.LAYER_DIM;
                break;
        }
        return layer;
    }
}
