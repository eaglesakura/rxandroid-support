package com.eaglesakura.android.rx.error;

public class TaskTimeoutException extends RxTaskException {
    public TaskTimeoutException() {
    }

    public TaskTimeoutException(String detailMessage) {
        super(detailMessage);
    }

    public TaskTimeoutException(String detailMessage, Throwable throwable) {
        super(detailMessage, throwable);
    }

    public TaskTimeoutException(Throwable throwable) {
        super(throwable);
    }
}
