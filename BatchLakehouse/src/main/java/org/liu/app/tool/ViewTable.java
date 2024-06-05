package org.liu.app.tool;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;
import org.liu.app.BaseApp;
import org.liu.constant.Constant;
import org.liu.util.Utils;

public class ViewTable extends BaseApp {
    public static void main(String[] args) {
        new ViewTable().run(args, 4);
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
        DeltaTable.forName(spark, Utils.getTableName(layer, args[1])).toDF().show(false);
    }
}
