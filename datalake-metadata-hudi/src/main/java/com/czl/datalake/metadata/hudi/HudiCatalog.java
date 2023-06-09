package com.czl.datalake.metadata.hudi;

import com.czl.datalake.metadata.api.catalog.DataLakeCatalog;
import com.czl.datalake.metadata.api.entity.TableHistory;
import com.czl.datalake.metadata.api.exception.MetaDataException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/29
 * Description:
 */
public class HudiCatalog extends DataLakeCatalog {

    private HoodieTableMetaClient metaClient;

    @Override
    public void initialize(Configuration conf, String lakePath) {
        metaClient = getMetaClient(conf, lakePath);
    }

    @Override
    public List<String> getPartitionFields(Configuration conf, String lakePath) {
        initialize(conf, lakePath);
        Option<String[]> fields = metaClient.getTableConfig().getPartitionFields();
        String[] partitions = fields.orElse(new String[0]);
        return Arrays.asList(partitions);
    }

    @Override
    public List<TableHistory> getTableHistory(Configuration conf, String lakePath) {
        initialize(conf, lakePath);
        List<TableHistory> list = new ArrayList<>();
        HoodieTimeline allHoodieCommitsTimeline = getAllHoodieCommitsTimeline(metaClient);
        int counts = allHoodieCommitsTimeline.countInstants();
        for (int i = 0; i < counts; i++) {
            try {
                String operationType = getOperationType(allHoodieCommitsTimeline, i);
                String time = getInstantTime(allHoodieCommitsTimeline, i);
                TableHistory tableHistory = new TableHistory(
                        (long) i,
                        operationType,
                        Timestamp.valueOf(time));
                list.add(tableHistory);
            } catch (IOException exception) {
                throw new MetaDataException("query HoodieTimeline failed", exception);
            }
        }
        return list;
    }

    private HoodieTableMetaClient getMetaClient(Configuration conf, String basePath) {
        return HoodieTableMetaClient
                .builder()
                .setConf(conf)
                .setBasePath(basePath)
                .setLoadActiveTimelineOnLoad(true)
                .build();
    }

    private HoodieTimeline getAllHoodieCommitsTimeline(HoodieTableMetaClient client) {
        return client.getActiveTimeline().getAllCommitsTimeline();
    }

    private String getOperationType(HoodieTimeline allCommitsTimeline, Integer n) throws IOException {
        HoodieInstant instant = allCommitsTimeline.nthInstant(n).get();
        byte[] details = allCommitsTimeline.getInstantDetails(instant).get();
        try {
            HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(details, HoodieCommitMetadata.class);
            return metadata.getOperationType().value();
        } catch (IOException exception) {
            throw new IOException(exception.getMessage());
        }
    }

    private String getInstantTime(HoodieTimeline allCommitsTimeline, Integer n) {
        HoodieInstant instant = allCommitsTimeline.nthInstant(n).get();
        return formatHoodieTime(instant.getTimestamp());
    }

    private String formatHoodieTime(String time) {
        StringBuilder builder = new StringBuilder(time);
        builder.insert(4,"-");
        builder.insert(7,"-");
        builder.insert(10," ");
        builder.insert(13,":");
        builder.insert(16,":");
        builder.insert(19,".");
        return builder.toString();
    }
}
