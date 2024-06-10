package org.liu.tool;

import org.apache.spark.sql.SparkSession;
import org.liu.app.BaseApp;
import org.liu.util.Utils;

import static org.liu.util.Utils.getLayer;

public class DeleteTable extends BaseApp {
    public static void main(String[] args) {
        new DeleteTable().run(args, 1);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        String layer = getLayer(args[0]);
        String table = args[1];
        spark.sql("DROP TABLE IF EXISTS " + Utils.getTableName(layer, table));
    }
}
