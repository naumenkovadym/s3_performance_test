package com.s3_test.task;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import static software.amazon.awssdk.transfer.s3.SizeConstant.MB;

public class UploadFileTask implements Callable<Long> {

    private final S3TransferManager transferManager;
    private final Path testFilePath;
    private final String fileName;
    private final String outputBucket;
    private final String outputPrefix;

    public UploadFileTask(Path testFilePath, String fileName,
                          String outputBucket, String outputPrefix) {

        S3AsyncClient s3AsyncClient =
                S3AsyncClient.crtBuilder()
                        .minimumPartSizeInBytes(5 * MB)
                        .build();

        this.transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
        this.testFilePath = testFilePath;
        this.fileName = fileName;
        this.outputBucket = outputBucket;
        this.outputPrefix = outputPrefix;
    }

    @Override
    public Long call() {

        long timeStart = System.currentTimeMillis();

        UploadFileRequest uploadFileRequest =
                UploadFileRequest.builder()
                        .putObjectRequest(b -> b.bucket(outputBucket)
                                .key(outputPrefix + fileName)
                        )
                        .source(testFilePath)
                        .build();

        FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);

        fileUpload.completionFuture().join();

        return System.currentTimeMillis() - timeStart;
    }
}
