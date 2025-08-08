package org.apache.flink.cdc.connectors.hudi.sink.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A class to manage the bucket assignment index which maps partitions and bucket numbers to file
 * IDs for a single table. Each EventBucketStreamWriteFunction instance handles one table, so this
 * class only needs to track partitions within that table. The structure is: {@code PartitionPath ->
 * BucketId -> FileId}.
 */
public class BucketAssignmentIndex implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Index mapping partition paths to bucket-to-fileId mappings for a single table */
    private final Map<String, Map<Integer, String>> index;

    public BucketAssignmentIndex() {
        this.index = new HashMap<>();
    }

    /**
     * Retrieves the File ID for a given partition and bucket number.
     *
     * @param partition The partition path.
     * @param bucketNum The bucket number.
     * @return An Optional containing the file ID if it exists, otherwise an empty Optional.
     */
    public Optional<String> getFileId(String partition, int bucketNum) {
        return Optional.ofNullable(index.get(partition)).map(bucketMap -> bucketMap.get(bucketNum));
    }

    /**
     * Associates the specified file ID with the specified partition and bucket number.
     *
     * @param partition The partition path.
     * @param bucketNum The bucket number.
     * @param fileId The file ID to associate with the bucket.
     */
    public void putFileId(String partition, int bucketNum, String fileId) {
        index.computeIfAbsent(partition, k -> new HashMap<>()).put(bucketNum, fileId);
    }

    /**
     * Checks if the index contains mappings for the specified partition.
     *
     * @param partition The partition path.
     * @return true if the index contains a mapping for the partition, false otherwise.
     */
    public boolean containsPartition(String partition) {
        return index.containsKey(partition);
    }

    /**
     * Bootstraps the index for a new partition with a pre-loaded map of bucket numbers to file IDs.
     *
     * @param partition The partition path.
     * @param bucketToFileIDMap The map of bucket numbers to file IDs for the partition.
     */
    public void bootstrapPartition(String partition, Map<Integer, String> bucketToFileIDMap) {
        index.put(partition, bucketToFileIDMap);
    }

    /**
     * Gets the map from bucket number to file ID for a given partition. Creates and returns an
     * empty map if one does not exist.
     *
     * @param partition The partition path.
     * @return The map of bucket numbers to file IDs.
     */
    public Map<Integer, String> getBucketToFileIdMap(String partition) {
        return index.computeIfAbsent(partition, k -> new HashMap<>());
    }

    /** Clears the entire index. */
    public void clear() {
        index.clear();
    }
}
