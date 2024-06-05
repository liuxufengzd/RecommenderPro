package org.liu.app.tool;

import org.apache.spark.sql.SparkSession;
import org.liu.app.BaseApp;
import org.liu.constant.Constant;
import org.liu.util.Utils;

public class DeleteTable extends BaseApp {
    public static void main(String[] args) {
        new DeleteTable().run(args, 1);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        String layer = args[0];
        switch (layer.toLowerCase()) {
            case "dwd":
                layer = Constant.LAYER_DWD;
                break;
            case "dws":
                layer = Constant.LAYER_DWS;
                break;
            default:
                layer = Constant.LAYER_ODS;
                break;
        }

        spark.sql("DROP TABLE IF EXISTS " + Utils.getTableName(layer, args[1]));
    }
}
