package com.czl.datalake.metadata.iceberg;

import com.czl.datalake.metadata.api.catalog.DataLakeCatalog;
import com.czl.datalake.metadata.api.entity.TableHistory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/28
 * Description:
 */
public class IcebergCatalog extends DataLakeCatalog {


    private Table icebergTable;

    @Override
    public void initialize(Configuration conf,String lakePath) {
        Path path = new Path(lakePath);
        String warehouse = path.getParent().getParent().toString();
        String db = path.getParent().getName();
        String table = path.getName();
        HadoopCatalog catalog = getHadoopCatalog(conf, warehouse);
        icebergTable = getIcebergTable(catalog, db, table);
    }

    @Override
    public List<String> getPartitionFields(Configuration conf, String lakePath) {
        initialize(conf, lakePath);
        List<String> list = new ArrayList<>();
        PartitionSpec spec = icebergTable.spec();
        spec.fields().forEach(partitionField -> list.add(partitionField.name()));
        return list;
    }

    @Override
    public List<TableHistory> getTableHistory(Configuration conf, String lakePath) {
        initialize(conf, lakePath);
        List<TableHistory> list = new ArrayList<>();
        icebergTable.snapshots().forEach(snapshot -> {
            TableHistory tableHistory = new TableHistory(
                    snapshot.snapshotId(),
                    snapshot.operation(),
                    new Timestamp(snapshot.timestampMillis()));
            list.add(tableHistory);
        });
        return list;
    }


    private Table getIcebergTable(HadoopCatalog catalog, String db, String table) {
        TableIdentifier identifier = TableIdentifier.of(db, table);
        return catalog.loadTable(identifier);
    }

    private HadoopCatalog getHadoopCatalog(Configuration conf, String warehousePath) {
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(conf);
        catalog.initialize("hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehousePath));
        return catalog;
    }
}
