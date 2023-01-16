package com.s3_test.task;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ResponseMetadata;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.internal.TransferManagerUtils;
import com.amazonaws.services.s3.transfer.internal.UploadPartRequestFactory;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.s3_test.service.S3Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;

public class UploadCallable implements Callable<UploadResult> {

    private static final Logger log = LoggerFactory.getLogger(S3Service.class);

    private final AmazonS3 s3;
    private final ExecutorService threadPool;
    private final PutObjectRequest origReq;
    private final int iterationNum;
    private final String fileName;

    private String multipartUploadId;

    public UploadCallable(AmazonS3 s3, ExecutorService threadPool, PutObjectRequest origReq, int iterationNum, String fileName) {
        this.s3 = s3;
        this.threadPool = threadPool;
        this.origReq = origReq;
        this.iterationNum = iterationNum;
        this.fileName = fileName;
    }

    @Override
    public UploadResult call() {
        long optimalPartSize = getOptimalPartSize();

        try {
            multipartUploadId = initiateMultipartUpload(origReq);
            UploadPartRequestFactory requestFactory = new UploadPartRequestFactory(origReq, multipartUploadId, optimalPartSize);
            return uploadPartsInSeries(requestFactory);
        } catch (Exception e) {
            performAbortMultipartUpload();
            throw e;
        } finally {
            if (origReq.getInputStream() != null) {
                try {
                    origReq.getInputStream().close();
                } catch (Exception e) {
                    log.warn("Unable to cleanly close input stream: " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Computes and returns the optimal part size for the upload.
     */
    private long getOptimalPartSize() {
        return TransferManagerUtils.calculateOptimalPartSize(origReq, new TransferManagerConfiguration());
    }

    /**
     * Initiates a multipart upload and returns the upload id
     */
    private String initiateMultipartUpload(PutObjectRequest origReq) {

        InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(origReq.getBucketName(), origReq.getKey())
                .withCannedACL(origReq.getCannedAcl())
                .withObjectMetadata(origReq.getMetadata());

        req.withTagging(origReq.getTagging());

        TransferManager.appendMultipartUserAgent(req);

        req.withAccessControlList(origReq.getAccessControlList())
                .withRequesterPays(origReq.isRequesterPays())
                .withStorageClass(origReq.getStorageClass())
                .withRedirectLocation(origReq.getRedirectLocation())
                .withSSECustomerKey(origReq.getSSECustomerKey())
                .withSSEAwsKeyManagementParams(origReq.getSSEAwsKeyManagementParams())
                .withGeneralProgressListener(origReq.getGeneralProgressListener())
                .withRequestMetricCollector(origReq.getRequestMetricCollector())
        ;

        long timeStart = System.currentTimeMillis();
        String uploadId = s3.initiateMultipartUpload(req).getUploadId();
        long executionMillis = System.currentTimeMillis() - timeStart;

        S3ResponseMetadata s3ResponseMetadata = s3.getCachedResponseMetadata(req);
        log.info("File name: " + fileName +
                ". Iteration: " + iterationNum +
                ". Initiated new multipart upload requestId: " + s3ResponseMetadata.getRequestId() +
                ". Execution millis: " + executionMillis);

        return uploadId;
    }

    /**
     * Performs an
     * {@link AmazonS3#abortMultipartUpload(AbortMultipartUploadRequest)}
     * operation for the given multi-part upload.
     */
    void performAbortMultipartUpload() {
        if (multipartUploadId == null) {
            return;
        }
        try {
            AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(origReq.getBucketName(), origReq.getKey(),
                    multipartUploadId)
                    .withRequesterPays(origReq.isRequesterPays());
            s3.abortMultipartUpload(abortRequest);
        } catch (Exception e2) {
            log.info(
                    "Unable to abort multipart upload, you may need to manually remove uploaded parts: "
                            + e2.getMessage(), e2);
        }
    }

    /**
     * Uploads all parts in the request in serial in this thread, then completes
     * the upload and returns the result.
     */
    private UploadResult uploadPartsInSeries(UploadPartRequestFactory requestFactory) {

        final List<PartETag> partETags = new ArrayList<PartETag>();

        while (requestFactory.hasMoreRequests()) {
            if (threadPool.isShutdown()) throw new CancellationException("TransferManager has been shutdown");
            UploadPartRequest uploadPartRequest = requestFactory.getNextUploadPartRequest();
            // Mark the stream in case we need to reset it
            InputStream inputStream = uploadPartRequest.getInputStream();
            if (inputStream != null && inputStream.markSupported()) {
                if (uploadPartRequest.getPartSize() >= Integer.MAX_VALUE) {
                    inputStream.mark(Integer.MAX_VALUE);
                } else {
                    inputStream.mark((int) uploadPartRequest.getPartSize());
                }
            }
            long timeStart = System.currentTimeMillis();
            partETags.add(s3.uploadPart(uploadPartRequest).getPartETag());
            long executionMillis = System.currentTimeMillis() - timeStart;

            S3ResponseMetadata s3ResponseMetadata = s3.getCachedResponseMetadata(uploadPartRequest);
            log.info("File name: " + fileName +
                    ". Iteration: " + iterationNum +
                    ". Part upload requestId: " + s3ResponseMetadata.getRequestId() +
                    ". Execution millis: " + executionMillis);
        }

        CompleteMultipartUploadRequest req =
                new CompleteMultipartUploadRequest(
                        origReq.getBucketName(), origReq.getKey(), multipartUploadId,
                        partETags)
                        .withRequesterPays(origReq.isRequesterPays())
                        .withGeneralProgressListener(origReq.getGeneralProgressListener())
                        .withRequestMetricCollector(origReq.getRequestMetricCollector());

        long timeStart = System.currentTimeMillis();
        CompleteMultipartUploadResult res = s3.completeMultipartUpload(req);
        long executionMillis = System.currentTimeMillis() - timeStart;

        S3ResponseMetadata s3ResponseMetadata = s3.getCachedResponseMetadata(req);
        log.info("File name: " + fileName +
                ". Iteration: " + iterationNum +
                ". Complete multipart upload requestId: " + s3ResponseMetadata.getRequestId() +
                ". Execution millis: " + executionMillis);

        UploadResult uploadResult = new UploadResult();
        uploadResult.setBucketName(res.getBucketName());
        uploadResult.setKey(res.getKey());
        uploadResult.setETag(res.getETag());
        uploadResult.setVersionId(res.getVersionId());
        return uploadResult;
    }
}
