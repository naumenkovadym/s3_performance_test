package com.s3_test.task;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.s3_test.Main;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.*;

public class UploadFileTask implements Callable<Long> {

    private final AmazonS3 amazonS3;
    private final byte[] fileContent;
    private final String fileName;
    private final int iterationNum;
    private final String outputBucket;
    private final String outputPrefix;
    private final ExecutorService executorService;

    public UploadFileTask(AmazonS3 amazonS3, byte[] fileContent, String fileName, int iterationNum,
                          String outputBucket, String outputPrefix) {
        this.amazonS3 = amazonS3;
        this.fileContent = fileContent;
        this.fileName = fileName;
        this.iterationNum = iterationNum;
        this.outputBucket = outputBucket;
        this.outputPrefix = outputPrefix;
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public Long call() throws Exception {
        long timeStart = System.currentTimeMillis();

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(fileContent.length);

        InputStream inputStream = new ByteArrayInputStream(fileContent);

        PutObjectRequest putObjectRequest = new PutObjectRequest(outputBucket, outputPrefix + fileName, inputStream, objectMetadata);

        UploadCallable uploadCallable = new UploadCallable(amazonS3, executorService, putObjectRequest, iterationNum, fileName);

        UploadMonitor uploadMonitor = new UploadMonitor(uploadCallable, executorService);

        waitForUploadResult(uploadMonitor);

        long executionTime = System.currentTimeMillis() - timeStart;

        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(Main.UPLOAD_FILE_TIMEOUT_MIN, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ignored) {
        }

        return executionTime;
    }

    private void waitForUploadResult(UploadMonitor uploadMonitor)
            throws AmazonClientException, InterruptedException, ExecutionException {
        UploadResult result = null;
        while (!uploadMonitor.isDone() || result == null) {
            Future<?> f = uploadMonitor.getFuture();
            result = (UploadResult) f.get();
        }
    }
}
