package com.s3_test.task;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.transfer.model.UploadResult;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class UploadMonitor implements Callable<UploadResult> {

    private final UploadCallable multipartUploadCallable;
    private final AtomicReference<Future<UploadResult>> futureReference;

    private boolean isUploadDone = false;

    public UploadMonitor(UploadCallable multipartUploadCallable, ExecutorService threadPool) {
        this.multipartUploadCallable = multipartUploadCallable;
        this.futureReference = new AtomicReference<>(null);

        Future<UploadResult> thisFuture = threadPool.submit(this);
        this.futureReference.compareAndSet(null, thisFuture);
    }

    @Override
    public UploadResult call() {
        try {
            UploadResult result = multipartUploadCallable.call();
            markAllDone();
            return result;
        } catch (CancellationException e) {
            throw new SdkClientException("Upload canceled");
        }
    }

    public synchronized boolean isDone() {
        return isUploadDone;
    }

    private synchronized void markAllDone() {
        isUploadDone = true;
    }

    public Future<UploadResult> getFuture() {
        return futureReference.get();
    }
}
