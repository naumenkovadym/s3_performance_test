package com.s3_test.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3Service.class);

    private final AmazonS3 amazonS3;

    public S3Service(AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
    }

    public void deleteDirectory(String bucketName, String prefix) {

        try {
            List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>();

            ListObjectsV2Request request = new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(prefix);

            ListObjectsV2Result result;

            do {
                result = amazonS3.listObjectsV2(request);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    keysToDelete.add(new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()));
                }
                request.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());

            if (!keysToDelete.isEmpty()) {
                DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
                        .withKeys(keysToDelete)
                        .withQuiet(false);

                DeleteObjectsResult deleteObjectsResult = amazonS3.deleteObjects(multiObjectDeleteRequest);

                int successfulDeletes = deleteObjectsResult.getDeletedObjects().size();

                if (successfulDeletes != keysToDelete.size()) {
                    throw new RuntimeException(String.format("Not all keys deleted, had to delete: %d, deleted: %d",
                            keysToDelete.size(), successfulDeletes));
                }
            }

            log.info(String.format("Deleted %d objects in %s s3 path", keysToDelete.size(), prefix));
        } catch (Exception ex) {
            log.warn(String.format("Directory %s wasn't deleted", prefix));
        }
    }
}
