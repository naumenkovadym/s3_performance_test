package com.s3_test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.s3_test.model.FileUploadResult;
import com.s3_test.service.S3Service;
import com.s3_test.task.UploadFileTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(S3Service.class);

    private static final int NUMBER_OF_FILES = 50;
    private static final int FILE_SIZE_MB = 50 * 1024 * 1024;
    public static final long UPLOAD_FILE_TIMEOUT_MIN = 5L;

    private final AmazonS3 amazonS3;
    private final S3Service s3Service;
    private final String outputBucket;
    private final String outputPrefix;

    public Main(String outputBucket, String outputPrefix) {
        this.outputBucket = outputBucket;
        this.outputPrefix = outputPrefix;
        this.amazonS3 = AmazonS3ClientBuilder.standard().build();
        this.s3Service = new S3Service(amazonS3);
    }

    public static void main(String[] args) {
        log.info("Test application started");
        Main main = new Main(args[1], args[2]);
        List<byte[]> filesContent = main.createMockFiles();
        main.testS3(filesContent, Long.parseLong(args[0]));
    }

    private List<byte[]> createMockFiles() {
        List<byte[]> resultList = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_FILES; i++) {
            byte[] bytes = new byte[FILE_SIZE_MB];
            new Random().nextBytes(bytes);
            resultList.add(bytes);
        }

        return resultList;
    }

    private void testS3(List<byte[]> filesContent, long timeLimitSec) {
        List<FileUploadResult> fileUploadResults = new ArrayList<>();

        int iterationNum = 0;

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        while (true) {
            for (int i = 0; i < filesContent.size(); i++) {
                String fileName = "test_file_" + i;
                Future<Long> futureUploadMillis = executorService.submit(
                        new UploadFileTask(amazonS3, filesContent.get(i), fileName, iterationNum,
                                outputBucket, outputPrefix));
                fileUploadResults.add(new FileUploadResult(fileName, futureUploadMillis, iterationNum));
            }
            checkRunningTime(fileUploadResults, timeLimitSec);
            fileUploadResults.clear();
            s3Service.deleteDirectory(outputBucket, outputPrefix);
            try {
                executorService.shutdown();
                if (!executorService.awaitTermination(UPLOAD_FILE_TIMEOUT_MIN, TimeUnit.MINUTES)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException ignored) {
            }
            executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            log.info("=============================");
            log.info("=============================");
            sleepSec(10);
            iterationNum++;
        }
    }

    private void checkRunningTime(List<FileUploadResult> fileUploadResults, long timeLimitSec) {

        fileUploadResults.forEach(fileUploadResult -> {
            try {
                long runningTimeSec = fileUploadResult.getFutureUploadMillis()
                        .get(UPLOAD_FILE_TIMEOUT_MIN, TimeUnit.MINUTES) / 1000;
                if (runningTimeSec > timeLimitSec) {
                    log.warn("File name: " + fileUploadResult.getFileName() +
                            ". Iteration: " + fileUploadResult.getIterationNum() +
                            ". Running time is not ok: " + runningTimeSec + " sec");
                } else {
                    log.info("File name: " + fileUploadResult.getFileName() +
                            ". Iteration: " + fileUploadResult.getIterationNum() +
                            ". Running time is ok: " + runningTimeSec + " sec");
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("File upload exception: ", e);
            } catch (TimeoutException e) {
                log.error("Can't upload file in 5 min");
            }
        });
    }

    private static void sleepSec(int sec) {
        try {
            TimeUnit.SECONDS.sleep(sec);
        } catch (InterruptedException var2) {
            Thread.currentThread().interrupt();
        }
    }
}
