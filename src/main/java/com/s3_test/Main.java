package com.s3_test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.s3_test.model.FileUploadResult;
import com.s3_test.service.S3Service;
import com.s3_test.task.UploadFileTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static software.amazon.awssdk.transfer.s3.SizeConstant.MB;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final int NUMBER_OF_FILES = 50;
    private static final int FILE_SIZE_MB = 50 * 1024 * 1024;
    private static final long UPLOAD_FILE_TIMEOUT_MIN = 5L;

    private final S3Service s3Service;
    private final String outputBucket;
    private final String outputPrefix;
    private final S3TransferManager transferManager;

    public Main(String outputBucket, String outputPrefix) {
        this.outputBucket = outputBucket;
        this.outputPrefix = outputPrefix;
        S3Client s3Client = S3Client.builder().build();
        this.s3Service = new S3Service(s3Client);

        S3AsyncClient s3AsyncClient =
                S3AsyncClient.crtBuilder()
                        .minimumPartSizeInBytes(5 * MB)
                        .build();

        //if I create it in the UploadFileTask, I get AWS_ERROR_PRIORITY_QUEUE_EMPTY after 18 iterations
        this.transferManager = S3TransferManager.builder()
                .s3Client(s3AsyncClient)
                .build();
    }

    public static void main(String[] args) throws IOException {
        log.info("Test application started");
        Main main = new Main(args[1], args[2]);
        List<Path> testFilePaths = main.createMockFiles();
        main.testS3(testFilePaths, Long.parseLong(args[0]));
    }

    private List<Path> createMockFiles() throws IOException {
        FileSystem fs = Jimfs.newFileSystem(Configuration.unix());
        Path foo = fs.getPath("/s3_test_files");
        Files.createDirectory(foo);

        List<Path> testFilePaths = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_FILES; i++) {
            byte[] bytes = new byte[FILE_SIZE_MB];
            new Random().nextBytes(bytes);

            Path filePath = foo.resolve("file_" + i);
            Files.write(filePath, bytes);

            testFilePaths.add(filePath);
        }

        return testFilePaths;
    }

    private void testS3(List<Path> testFilePaths, long timeLimitSec) {
        List<FileUploadResult> fileUploadResults = new ArrayList<>();

        int iterationNum = 0;

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        while (true) {
            for (int i = 0; i < testFilePaths.size(); i++) {
                String fileName = "test_file_" + i;
                Future<Long> futureUploadMillis = executorService.submit(
                        new UploadFileTask(transferManager, testFilePaths.get(i), fileName, outputBucket, outputPrefix));
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
