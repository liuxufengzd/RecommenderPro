package org.liu.tool;

import org.apache.spark.sql.SparkSession;
import org.liu.app.BaseApp;

import static org.liu.util.Utils.getLayer;

public class DatabaseLoader extends BaseApp {
    public static void main(String[] args) {
        new DatabaseLoader().run(args, 1);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        String layer = getLayer(args[0]);
        String sourceTable = args[1];
        String sinkTable = args[2];
        DatabaseWriter(TableReader(layer, sourceTable), sinkTable);
    }
}
