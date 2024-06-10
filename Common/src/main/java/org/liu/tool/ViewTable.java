package org.liu.tool;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;
import org.liu.app.BaseApp;
import org.liu.util.Utils;

import static org.liu.util.Utils.getLayer;

public class ViewTable extends BaseApp {
    public static void main(String[] args) {
        new ViewTable().run(args, 4);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        String layer = getLayer(args[0]);
        String table = args[1];
        DeltaTable.forName(spark, Utils.getTableName(layer, table)).toDF().show(false);
    }
}
