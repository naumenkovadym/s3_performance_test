package com.s3_test.task;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.Callable;

public class UploadFileTask implements Callable<Long> {

    private final TransferManager transferManager;
    private final byte[] fileContent;
    private final String fileName;
    private final String outputBucket;
    private final String outputPrefix;

    public UploadFileTask(byte[] fileContent, String fileName,
                          String outputBucket, String outputPrefix) {
        this.transferManager = TransferManagerBuilder.defaultTransferManager();
        this.fileContent = fileContent;
        this.fileName = fileName;
        this.outputBucket = outputBucket;
        this.outputPrefix = outputPrefix;
    }

    @Override
    public Long call() throws InterruptedException {

        long timeStart = System.currentTimeMillis();

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(fileContent.length);

        InputStream inputStream = new ByteArrayInputStream(fileContent);

        PutObjectRequest putObjectRequest = new PutObjectRequest(outputBucket, outputPrefix + fileName, inputStream, objectMetadata);

        Upload upload = transferManager.upload(putObjectRequest);
        upload.waitForUploadResult();

        return System.currentTimeMillis() - timeStart;
    }
}
