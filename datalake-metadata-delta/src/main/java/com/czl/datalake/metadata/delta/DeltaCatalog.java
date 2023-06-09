package com.czl.datalake.metadata.delta;

import com.czl.datalake.metadata.api.catalog.DataLakeCatalog;
import com.czl.datalake.metadata.api.entity.TableHistory;
import com.czl.datalake.metadata.api.exception.MetaDataException;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.CommitInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/29
 * Description:
 */
public class DeltaCatalog extends DataLakeCatalog {


    private static final Pattern deltaFilePattern = Pattern.compile("\\d+\\.json");

    private static final Pattern checkpointFilePattern = Pattern.compile("\\d+\\.checkpoint(\\.\\d+\\.\\d+)?\\.parquet");

    private DeltaLog deltaLog;

    @Override
    public void initialize(Configuration conf, String lakePath) {
        deltaLog = DeltaLog.forTable(conf, lakePath);
    }

    @Override
    public List<String> getPartitionFields(Configuration conf, String lakePath) {
        initialize(conf, lakePath);
        Snapshot snapshot = deltaLog.snapshot();
        return snapshot.getMetadata().getPartitionColumns();
    }

    @Override
    public List<TableHistory> getTableHistory(Configuration conf, String lakePath) {
        initialize(conf, lakePath);
        long currentVersion = getDeltaTableCurrentVersion(deltaLog);
        List<String> paths = listFilePah(conf, lakePath + "/_delta_log");
        Long earliestVersion = getEarliestAvailableCommitVersion(paths);
        if (earliestVersion < 0) {
            throw new MetaDataException("find delta log earliest version failed");
        }
        List<TableHistory> list = new ArrayList<>();
        for (long i = earliestVersion; i < currentVersion + 1; i++) {
            CommitInfo commitInfo = getDeltaCommitInfo(i);
            TableHistory tableHistory = new TableHistory(
                    commitInfo.getVersion().orElse(null),
                    commitInfo.getOperation(),
                    commitInfo.getTimestamp());
            list.add(tableHistory);
        }
        return list;
    }

    private CommitInfo getDeltaCommitInfo(long version) {
        return deltaLog.getCommitInfoAt(version);
    }

    private Long getDeltaTableCurrentVersion(DeltaLog deltaLog) {
        return deltaLog.snapshot().getVersion();
    }

    private Boolean isDeltaFile(String path) {
        return deltaFilePattern.matcher(getFileName(path)).matches();
    }

    private Boolean isCheckpointFile(String path) {
        return checkpointFilePattern.matcher(getFileName(path)).matches();
    }

    private Long deltaVersion(String path) {
        String fileName = getFileName(path);
        return Long.valueOf(stripSuffix(fileName));
    }

    private Long checkpointVersion(String path) {
        return Long.valueOf(getFileName(path).split("\\.")[0]);
    }

    private String getFileName(String path) {
        int i = path.lastIndexOf("/");
        return path.substring(i + 1);
    }

    private String stripSuffix(String string) {
        return string.endsWith(".json") ? string.substring(0, string.length() - ".json".length()) : string;
    }

    private List<String> listFilePah(Configuration conf, String lakePath) {
        try {
            FileSystem fileSystem = FileSystem.get(new URI(lakePath), conf);
            return Arrays.stream(fileSystem.listStatus(new Path(lakePath)))
                    .map(f -> f.getPath().toString())
                    .collect(Collectors.toList());
        } catch (IOException | URISyntaxException exception) {
            throw new MetaDataException(exception);
        }
    }

    private Optional<Integer> numCheckpointParts(String path) {
        String[] segments = getFileName(path).split("\\.");
        return (segments.length !=5) ? Optional.empty() : Optional.of(Integer.valueOf(segments[3]));
    }

    private Long getEarliestAvailableCommitVersion(List<String> logPaths) {
        Map<Map<Long, Integer>, Integer> checkpointMap = new HashMap<>();
        long smallestDeltaVersion = Long.MAX_VALUE;
        Optional<Long> lastCompleteCheckpoint = Optional.empty();

        for (String nextFilePath : logPaths) {
            if (isDeltaFile(nextFilePath)) {
                Long version = deltaVersion(nextFilePath);
                if (version == 0L) {
                    return version;
                }
                smallestDeltaVersion = Math.min(version, smallestDeltaVersion);
                if (lastCompleteCheckpoint.isPresent() && lastCompleteCheckpoint.get() >= smallestDeltaVersion) {
                    return lastCompleteCheckpoint.get();
                }
            } else if (isCheckpointFile(nextFilePath)) {
                Long cpVersion = checkpointVersion(nextFilePath);
                Optional<Integer> parts = numCheckpointParts(nextFilePath);
                if (!parts.isPresent()) {
                    lastCompleteCheckpoint = Optional.of(cpVersion);
                } else {
                    Integer numParts = parts.get();
                    Map<Long, Integer> longIntegerMap = Collections.singletonMap(cpVersion, numParts);
                    Integer preCount = checkpointMap.getOrDefault(longIntegerMap, 0);
                    if (numParts == preCount + 1) {
                        lastCompleteCheckpoint = Optional.of(cpVersion);
                    }
                    checkpointMap.put(Collections.singletonMap(cpVersion, numParts), preCount + 1);
                }
            }
        }
        if (lastCompleteCheckpoint.isPresent() && lastCompleteCheckpoint.get() >= smallestDeltaVersion) {
            return lastCompleteCheckpoint.get();
        } else {
            return -1L;
        }
    }
}
