package com.s3_test.model;

import java.util.concurrent.Future;

public class FileUploadResult {
    private final String fileName;
    private final Future<Long> futureUploadMillis;
    private final Integer iterationNum;

    public FileUploadResult(String fileName, Future<Long> futureUploadMillis, Integer iterationNum) {
        this.fileName = fileName;
        this.futureUploadMillis = futureUploadMillis;
        this.iterationNum = iterationNum;
    }

    public String getFileName() {
        return fileName;
    }

    public Future<Long> getFutureUploadMillis() {
        return futureUploadMillis;
    }

    public Integer getIterationNum() {
        return iterationNum;
    }
}
