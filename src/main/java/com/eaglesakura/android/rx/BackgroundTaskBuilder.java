package com.eaglesakura.android.rx;

import com.eaglesakura.android.rx.error.TaskCanceledException;

import android.app.Dialog;
import android.os.Looper;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentActivity;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.android.schedulers.AndroidSchedulers;
import rx.functions.Action1;

/**
 *
 */
public class BackgroundTaskBuilder<T> {
    final PendingCallbackQueue mController;

    Observable<T> mObservable;

    /**
     * 標準ではプロセス共有スレッドで実行される
     */
    ExecuteTarget mThreadTarget = ExecuteTarget.GlobalParallel;

    /**
     * Task
     */
    BackgroundTask mTask = new BackgroundTask<>();

    /**
     * チェーンの親となるビルダー
     */
    final BackgroundTaskBuilder<T> mParentBuilder;

    /**
     * タスクをスタート済みであればtrue
     */
    boolean mStartedTask = false;

    public BackgroundTaskBuilder(PendingCallbackQueue subscriptionController) {
        this(null, subscriptionController);
    }

    BackgroundTaskBuilder(BackgroundTaskBuilder parent, PendingCallbackQueue subscriptionController) {
        mParentBuilder = parent;
        mController = subscriptionController;
        mTask.mCallbackQueue = mController;
//        if (parent != null) {
//            // キャンセルシグナルを引き継ぐ
//            mTask.mUserCancelSignals.add(parent.mTask.mUserCancelSignals);
//        }
    }

    /**
     * 処理対象のスレッドを指定する
     */
    public BackgroundTaskBuilder<T> executeOn(ExecuteTarget target) {
        mThreadTarget = target;
        return this;
    }


    /**
     * 処理対象のスレッドを指定する
     */
    @Deprecated
    public BackgroundTaskBuilder<T> subscribeOn(SubscribeTarget target) {
        mThreadTarget = target.asExecuteTarget();
        return this;
    }

    /**
     * コールバック対象のタイミングを指定する
     */
    @Deprecated
    public BackgroundTaskBuilder<T> observeOn(ObserveTarget target) {
        mTask.mCallbackTime = target.asCallbackTarget();
        return this;
    }

    /**
     * コールバック対象のタイミングを指定する
     */
    public BackgroundTaskBuilder<T> callbackOn(CallbackTime target) {
        mTask.mCallbackTime = target;
        return this;
    }

    /**
     * ユーザのキャンセルチェックを有効化する
     */
    public BackgroundTaskBuilder<T> cancelSignal(BackgroundTask.Signal signal) {
        mTask.mUserCancelSignals.add(signal);
        return this;
    }

    public BackgroundTaskBuilder<T> cancelSignal(FragmentActivity activity) {
        return cancelSignal(task -> activity == null || activity.isFinishing());
    }

    public BackgroundTaskBuilder<T> cancelSignal(Fragment fragment) {
        return cancelSignal(task -> {
            if (fragment == null) {
                return true;
            }

            FragmentActivity activity = fragment.getActivity();
            return activity == null || activity.isFinishing();
        });
    }

    /**
     * ユーザーのキャンセルチェックをダイアログと同期する
     */
    public BackgroundTaskBuilder<T> cancelSignal(Dialog dialog) {
        return cancelSignal(task -> !dialog.isShowing());
    }

    /**
     * ダイアログに合わせてキャンセルチェックとキャンセル挙動を設定する
     */
    public BackgroundTaskBuilder<T> cancelSignal(Dialog dialog, BackgroundTask.Action0<T> callback) {
        return canceled(callback).
                cancelSignal(task -> !dialog.isShowing());
    }

    /**
     * 処理にタイムアウトを付与する
     */
    public BackgroundTaskBuilder<T> timeout(long timeoutMs) {
        mObservable.timeout(timeoutMs, TimeUnit.MILLISECONDS);
        mTask.mTimeoutMs = timeoutMs;
        return this;
    }

    /**
     * タスク名を指定する。
     * <p>
     * タスク名はそのままスレッド名として利用される。
     */
    public BackgroundTaskBuilder<T> name(String name) {
        mTask.mName = name;
        return this;
    }

    /**
     * エラー時のダミー戻り値を指定する。
     */
    public BackgroundTaskBuilder<T> errorReturn(BackgroundTask.ErrorReturn<T> action) {
        mObservable.onErrorReturn(err -> action.call(err, (BackgroundTask<T>) mTask));
        return this;
    }

    /**
     * リトライの最大回数を指定する
     */
    public BackgroundTaskBuilder<T> retry(int num) {
        mObservable.retry(num);
        return this;
    }

    /**
     * リトライチェック関数を指定する
     */
    public BackgroundTaskBuilder<T> retrySignal(BackgroundTask.Signal<T> action) {
        mObservable.retry((num, error) -> action.is(mTask));
        return this;
    }

    /**
     * 詳細なリトライチェック関数を指定する
     */
    public BackgroundTaskBuilder<T> retry(BackgroundTask.RetrySignal<T> action) {
        mObservable.retry((num, err) -> action.is(mTask, num, err));
        return this;
    }

    /**
     * スレッド名を指定する
     * <p>
     * 指定されていない場合は何もしない。
     */
    void bindThreadName() {
        if (mTask.mName == null) {
            return;
        }

        Thread current = Thread.currentThread();
        if (current.equals(Looper.getMainLooper().getThread())) {
            // UIスレッド名は変更できない
            return;
        }

        try {
            Thread.currentThread().setName("RxTask::" + mTask.mName);
        } catch (Exception e) {

        }
    }

    /**
     * 非同期処理を指定する
     */
    public BackgroundTaskBuilder<T> async(BackgroundTask.Async<T> subscribe) {
        mObservable = Observable.create((Subscriber<? super T> it) -> {
            synchronized (mTask) {
                mTask.mState = BackgroundTask.State.Running;
                bindThreadName();
            }

            //  非同期処理中はロックを外す
            T result;
            try {
                result = subscribe.call((BackgroundTask<T>) mTask);

                if (mTask.isCanceled()) {
                    throw new TaskCanceledException();
                }
            } catch (Throwable e) {
                it.onError(e);
                return;
            }

            // 実行完了をコールする
            synchronized (mTask) {
                it.onNext(result);
                it.onCompleted();
            }
        })
                .subscribeOn(mController.getThreadController().getScheduler(mThreadTarget))
                .observeOn(AndroidSchedulers.mainThread());
//                .unsubscribeOn(AndroidSchedulers.mainThread());
        return this;
    }

    /**
     * Observableを直接更新する
     */
    public BackgroundTaskBuilder<T> update(final Action1<Observable<T>> callback) {
        callback.call(mObservable);
        return this;
    }

    /**
     * 戻り値からの処理を記述する
     */
    public BackgroundTaskBuilder<T> completed(BackgroundTask.Action1<T> callback) {
        mTask.completed(callback);
        return this;
    }

    /**
     * エラーハンドリングを記述する
     */
    public BackgroundTaskBuilder<T> failed(BackgroundTask.ErrorAction<T> callback) {
        mTask.failed(callback);
        return this;
    }

    /**
     * キャンセル時のハンドリングを設定する
     */
    public BackgroundTaskBuilder<T> canceled(BackgroundTask.Action0<T> callback) {
        mTask.canceled(callback);
        return this;
    }

    /**
     * 終了時の処理を記述する
     */
    public BackgroundTaskBuilder<T> finalized(BackgroundTask.Action0<T> callback) {
        mTask.finalized(callback);
        return this;
    }

    /**
     * 現在構築中のタスクが正常終了した後に、連続して呼び出されるタスクを生成する。
     */
    public <R> BackgroundTaskBuilder<R> chain(BackgroundTask.AsyncChain<T, R> action) {
        BackgroundTaskBuilder<R> result = new BackgroundTaskBuilder<R>(this, mController)
                .executeOn(mThreadTarget)
                .callbackOn(mTask.mCallbackTime)
                .async((BackgroundTask<R> chainTask) -> action.call((T) mTask.getResult(), chainTask));

        mTask.mChainTask = result;
        return result;
    }

    public boolean isStartedTask() {
        return mStartedTask;
    }

    /**
     * セットアップを完了し、処理を開始する
     */
    public BackgroundTask<T> start() {
        // 実行準備する
        mTask.mState = BackgroundTask.State.Pending;
        // キャンセルを購読対象と同期させる
        mTask.mSubscribeCancelSignal = (task) -> mController.isCanceled(mTask.mCallbackTime);

        if (mParentBuilder != null && !mParentBuilder.isStartedTask()) {
            // 親がいるなら、親を開始する
            mParentBuilder.start();
        } else {
            if (isStartedTask()) {
                // 既にタスクが起動済みのため、再度起動することはできない
                throw new IllegalStateException();
            }
            mStartedTask = true;
            // 開始タイミングをズラす
            mController.getHandler().post(() -> {
                final Subscription subscribe = mObservable.subscribe(
                        // next = completeed
                        next -> {
                            mTask.setResult(next);
                        },
                        // error
                        error -> {
                            mTask.setError(error);
                        }
                );

                // 購読対象に追加
                mController.add(mTask.mCallbackTime, subscribe);
            });
        }
        return mTask;
    }
}
