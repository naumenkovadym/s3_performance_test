package com.s3_test.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3Service.class);

    private final S3Client s3Client;

    public S3Service(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public void deleteDirectory(String bucketName, String prefix) {

        int deletedObjects = 0;

        try {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .build();

            ListObjectsV2Response response;

            do {
                response = s3Client.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : response.contents()) {
                    DeleteObjectRequest request = DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Object.key())
                            .build();
                    s3Client.deleteObject(request);
                    deletedObjects++;
                }

            } while (response.isTruncated());

            log.info(String.format("Deleted %d objects in %s s3 path", deletedObjects, prefix));
        } catch (Exception ex) {
            log.warn(String.format("Directory %s wasn't deleted", prefix));
        }
    }
}
