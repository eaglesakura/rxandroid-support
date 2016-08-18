package com.eaglesakura.android.rx;

import com.eaglesakura.android.rx.error.RxTaskException;
import com.eaglesakura.android.rx.error.TaskCanceledException;
import com.eaglesakura.android.rx.error.TaskTimeoutException;

import java.util.HashSet;
import java.util.Set;

import rx.Subscription;

/**
 * 非同期実行されるタスクを管理する
 *
 * タスクは StateLinkQueue によってコールバック管理され、必要に応じてコールバックの保留やキャンセルが行われる。
 */
public class BackgroundTask<T> {

    /**
     * コールバック管理
     */
    PendingCallbackQueue mCallbackQueue;

    /**
     * 外部から指定されたキャンセルチェック
     */
    Set<Signal> mUserCancelSignals = new HashSet<>();

    /**
     * 購読対象からのキャンセルチェック
     */
    Signal mSubscribeCancelSignal;

    /**
     * 受信したエラー
     */
    Throwable mError;

    /**
     * 戻り値
     */
    T mResult;

    /**
     * 現在の処理ステート
     */
    State mState = State.Building;

    /**
     * 購読データ
     */
    Subscription mSubscription;

    /**
     * コールバック対象を指定する
     * <p>
     * デフォルトはFire And Forget
     */
    CallbackTime mCallbackTime = CallbackTime.FireAndForget;

    /**
     * 完了時処理を記述する
     */
    BackgroundTask.Action1<T> mCompletedCallback;

    /**
     * キャンセル時の処理を記述する
     */
    BackgroundTask.Action0<T> mCancelCallback;

    /**
     * エラー時の処理を記述する
     */
    BackgroundTask.ErrorAction<T> mErrorCallback;

    /**
     * 最終的に必ず呼び出される処理
     */
    BackgroundTask.Action0 mFinalizeCallback;

    /**
     * チェーン実行されるタスク
     */
    BackgroundTaskBuilder mChainTask;

    /**
     * タスク名
     */
    String mName = "Task::" + getClass().getName();

    /**
     * デフォルトのタイムアウト指定
     */
    long mTimeoutMs = 1000 * 60 * 60;

    public enum State {
        /**
         * タスクを生成中
         */
        Building,

        /**
         * まだ実行されていない
         */
        Pending,

        /**
         * タスクが実行中
         */
        Running,

        /**
         * 完了
         */
        Finished,
    }

    BackgroundTask() {
    }

    /**
     * 現在のタスク状態を取得する
     */
    public State getState() {
        return mState;
    }

    /**
     * 戻り値を取得する
     */
    public T getResult() {
        return mResult;
    }

    /**
     * 指定時間だけウェイトをかける。
     * <p>
     * 途中でキャンセルされた場合は例外を投げて終了される
     */
    public void waitTime(long timeMs) throws RxTaskException {
        if (timeMs <= 0) {
            return;
        }

        final long START_TIME = System.currentTimeMillis();
        while ((System.currentTimeMillis() - START_TIME) < timeMs) {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
            }

            if (isCanceled()) {
                throw new TaskCanceledException();
            }
        }
    }

    /**
     * awaitを行い、結果を捨てる
     */
    public boolean safeAwait(long timeoutMs) {
        try {
            await(timeoutMs);
            return true;
        } catch (Throwable e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * awaitを行い、結果を捨てる
     */
    public void safeAwait() {
        try {
            await();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * 処理待ちを行う
     * <p>
     * timeout等やコールバックと同居するため、実装はただのwaitである。
     */
    public T await(long timeoutMs) throws Throwable {
        final long START_TIME = System.currentTimeMillis();
        while (!isFinished() && ((System.currentTimeMillis() - START_TIME) < timeoutMs)) {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
            }
        }

        throwIfError();

        if (!isFinished()) {
            throw new TaskTimeoutException();
        }

        return mResult;
    }

    /**
     * 処理の完了待ちを行う
     */
    public T await() throws Throwable {
        return await(mTimeoutMs);
    }

    /**
     * エラー内容を取得する
     */
    public Throwable getError() {
        return mError;
    }

    /**
     * エラーを持っていたら投げる
     */
    public void throwIfError() throws Throwable {
        if (mError != null) {
            throw mError;
        }
    }

    /**
     * タスクがキャンセルされていたら例外を投げて処理を強制終了する
     */
    public void throwIfCanceled() throws TaskCanceledException {
        if (isCanceled()) {
            throw new TaskCanceledException();
        }
    }

    /**
     * タスクがキャンセル状態であればtrue
     */
    public boolean isCanceled() {
        // キャンセルシグナルに一つでも引っかかったらtrue
        for (Signal signal : mUserCancelSignals) {
            if (signal.is(this)) {
                return true;
            }
        }

        if (mSubscribeCancelSignal != null & mSubscribeCancelSignal.is(this)) {
            return true;
        }

        if (mError != null && (mError instanceof TaskCanceledException)) {
            return true;
        }

        return false;
    }

    public boolean isFinished() {
        synchronized (this) {
            return mState == State.Finished;
        }
    }

    public boolean hasError() {
        synchronized (this) {
            return mError != null;
        }
    }

    private void handleCanceled() {
        if (mCancelCallback == null) {
            return;
        }
        mCancelCallback.call(this);
    }

    private void handleCompleted(T next) {
        if (mCompletedCallback == null) {
            return;
        }

        mCompletedCallback.call(next, this);
    }

    private void handleChain() {
        // 連続実行タスクが残っているなら、チェーンで実行を開始する
        if (mChainTask != null) {
            mChainTask.start();
            mChainTask = null;
        }
    }

    private void handleFailed(Throwable error) {
        // タスクがキャンセルされた場合
        if (mErrorCallback == null) {
            return;
        }

        mErrorCallback.call(error, this);
    }

    private void handleFinalize() {
        if (mFinalizeCallback == null) {
            return;
        }
        mFinalizeCallback.call(this);
    }


    public BackgroundTask<T> completed(Action1<T> completedCallback) {
        synchronized (this) {
            mCompletedCallback = completedCallback;

            if (mState == BackgroundTask.State.Finished) {
                // タスクが終わってしまっているので、ハンドリングも行う
                if (!isCanceled() && !hasError()) {
                    handleCompleted(getResult());
                }
            }

            return this;
        }
    }

    public BackgroundTask<T> canceled(Action0<T> cancelCallback) {
        synchronized (this) {
            mCancelCallback = cancelCallback;
            if (mState == BackgroundTask.State.Finished && isCanceled()) {
                handleCanceled();
            }
            return this;
        }
    }

    public BackgroundTask<T> failed(ErrorAction<T> errorCallback) {
        synchronized (this) {
            mErrorCallback = errorCallback;
            if (mState == BackgroundTask.State.Finished) {
                // タスクが終わってしまっているので、ハンドリングする
                if (!isCanceled() && hasError()) {
                    mCallbackQueue.run(mCallbackTime, () -> {
                        handleFailed(getError());
                    });
                }
            }
            return this;
        }
    }

    public BackgroundTask<T> finalized(Action0 finalizeCallback) {
        synchronized (this) {
            mFinalizeCallback = finalizeCallback;
            if (mState == State.Finished) {
                mCallbackQueue.run(mCallbackTime, () -> {
                    handleFinalize();
                });
            }
            return this;
        }
    }

    void setResult(T result) {
        synchronized (this) {
            mState = State.Finished;
            mResult = result;
        }

        mCallbackQueue.run(mCallbackTime, () -> {

            if (isCanceled()) {
                handleCanceled();
            } else {
                try {
                    handleCompleted(result);
                    handleFinalize();
                    // 次のタスクを実行開始する
                    handleChain();
                } catch (Throwable error) {
                    // Completed処理に失敗した
                    mResult = null;
                    setError(error);
                }
            }
        });
    }

    void setError(Throwable error) {
        synchronized (this) {
            mState = State.Finished;
            mError = error;
        }

        mCallbackQueue.run(mCallbackTime, () -> {

            if (isCanceled()) {
                handleCanceled();
            } else {
                handleFailed(error);
                handleFinalize();
            }
        });
    }

    /**
     * 非同期処理を記述する
     */
    public interface Async<T> {
        T call(BackgroundTask<T> task) throws Throwable;
    }

    /**
     * 非同期連続処理を記述する
     */
    public interface AsyncChain<T, R> {
        R call(T before, BackgroundTask<R> task) throws Throwable;
    }

    /**
     * コールバックを記述する
     */
    public interface Action0<T> {
        void call(BackgroundTask<T> task);
    }

    /**
     * 非同期処理後のコールバックを記述する
     */
    public interface Action1<T> {
        void call(T it, BackgroundTask<T> task);
    }


    /**
     * 非同期処理後のコールバックを記述する
     */
    public interface ErrorAction<T> {
        void call(Throwable it, BackgroundTask<T> task);
    }

    /**
     * 非同期処理後のコールバックを記述する
     */
    public interface ErrorReturn<T> {
        T call(Throwable it, BackgroundTask<T> task);
    }

    /**
     * 各種チェック用のコールバック関数
     * <p>
     * cancel()メソッドを呼び出すか、このコールバックがisCanceled()==trueになった時点でキャンセル扱いとなる。
     */
    public interface Signal<T> {
        /**
         * キャンセルする場合はtrueを返す
         */
        boolean is(BackgroundTask<T> task);
    }


    /**
     * リトライチェック用
     */
    public interface RetrySignal<T> {
        boolean is(BackgroundTask<T> task, int count, Throwable error);
    }
}
