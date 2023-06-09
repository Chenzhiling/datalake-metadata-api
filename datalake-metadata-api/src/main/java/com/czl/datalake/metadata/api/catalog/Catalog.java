package com.czl.datalake.metadata.api.catalog;

import com.czl.datalake.metadata.api.entity.TableHistory;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/28
 * Description:
 */
public interface Catalog {

    void initialize(Configuration conf, String lakePath);

    List<String> getPartitionFields(Configuration conf, String lakePath);

    List<TableHistory> getTableHistory(Configuration conf, String lakePath);
}
