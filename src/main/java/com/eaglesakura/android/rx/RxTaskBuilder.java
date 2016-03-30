package com.eaglesakura.android.rx;

import com.eaglesakura.android.rx.error.TaskCanceledException;

import android.os.Looper;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.android.schedulers.AndroidSchedulers;
import rx.functions.Action1;

/**
 *
 */
public class RxTaskBuilder<T> {
    final SubscriptionController mSubscription;

    Observable<T> mObservable;

    /**
     * 標準ではプロセス共有スレッドで実行される
     */
    SubscribeTarget mThreadTarget = SubscribeTarget.GlobalParallels;

    /**
     * Task
     */
    RxTask mTask = new RxTask<>();

    /**
     * チェーンの親となるビルダー
     */
    final RxTaskBuilder<T> mParentBuilder;

    /**
     * タスクをスタート済みであればtrue
     */
    boolean mStartedTask = false;

    public RxTaskBuilder(SubscriptionController subscriptionController) {
        this(null, subscriptionController);
    }

    RxTaskBuilder(RxTaskBuilder parent, SubscriptionController subscriptionController) {
        mParentBuilder = parent;
        mSubscription = subscriptionController;
        mTask.mSubscription = mSubscription;

        if (parent != null) {
            // キャンセルシグナルを引き継ぐ
            mTask.mUserCancelSignal = parent.mTask.mUserCancelSignal;
        }
    }

    /**
     * 処理対象のスレッドを指定する
     */
    public RxTaskBuilder<T> subscribeOn(SubscribeTarget target) {
        mThreadTarget = target;
        return this;
    }

    /**
     * コールバック対象のタイミングを指定する
     */
    public RxTaskBuilder<T> observeOn(ObserveTarget target) {
        mTask.mObserveTarget = target;
        return this;
    }

    /**
     * ユーザのキャンセルチェックを有効化する
     */
    public RxTaskBuilder<T> cancelSignal(RxTask.Signal signal) {
        mTask.mUserCancelSignal = signal;
        return this;
    }

    /**
     * 処理にタイムアウトを付与する
     */
    public RxTaskBuilder<T> timeout(long timeoutMs) {
        mObservable.timeout(timeoutMs, TimeUnit.MILLISECONDS);
        mTask.mTimeoutMs = timeoutMs;
        return this;
    }

    /**
     * タスク名を指定する。
     * <p>
     * タスク名はそのままスレッド名として利用される。
     */
    public RxTaskBuilder<T> name(String name) {
        mTask.mName = name;
        return this;
    }

    /**
     * エラー時のダミー戻り値を指定する。
     */
    public RxTaskBuilder<T> errorReturn(RxTask.ErrorReturn<T> action) {
        mObservable.onErrorReturn(err -> action.call(err, (RxTask<T>) mTask));
        return this;
    }

    /**
     * リトライの最大回数を指定する
     */
    public RxTaskBuilder<T> retry(int num) {
        mObservable.retry(num);
        return this;
    }

    /**
     * リトライチェック関数を指定する
     */
    public RxTaskBuilder<T> retrySignal(RxTask.Signal<T> action) {
        mObservable.retry((num, error) -> action.is(mTask));
        return this;
    }

    /**
     * 詳細なリトライチェック関数を指定する
     */
    public RxTaskBuilder<T> retry(RxTask.RetrySignal<T> action) {
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
    public RxTaskBuilder<T> async(RxTask.Async<T> subscribe) {
        mObservable = Observable.create((Subscriber<? super T> it) -> {
            synchronized (mTask) {
                try {
                    mTask.mState = RxTask.State.Running;

                    bindThreadName();

                    T result = subscribe.call((RxTask<T>) mTask);
                    if (mTask.isCanceled()) {
                        throw new TaskCanceledException();
                    } else {
                        it.onNext(result);
                        it.onCompleted();
                    }
                } catch (Throwable e) {
                    it.onError(e);
                }
            }
        })
                .subscribeOn(mSubscription.getThreadController().getScheduler(mThreadTarget))
                .observeOn(AndroidSchedulers.mainThread());
        return this;
    }

    /**
     * Observableを直接更新する
     */
    public RxTaskBuilder<T> update(final Action1<Observable<T>> callback) {
        callback.call(mObservable);
        return this;
    }

    /**
     * 戻り値からの処理を記述する
     */
    public RxTaskBuilder<T> completed(RxTask.Action1<T> callback) {
        mTask.completed(callback);
        return this;
    }

    /**
     * エラーハンドリングを記述する
     */
    public RxTaskBuilder<T> failed(RxTask.ErrorAction<T> callback) {
        mTask.failed(callback);
        return this;
    }

    /**
     * キャンセル時のハンドリングを設定する
     */
    public RxTaskBuilder<T> canceled(RxTask.Action0<T> callback) {
        mTask.canceled(callback);
        return this;
    }

    /**
     * 終了時の処理を記述する
     */
    public RxTaskBuilder<T> finalized(RxTask.Action0<T> callback) {
        mTask.finalized(callback);
        return this;
    }

    /**
     * 現在構築中のタスクが正常終了した後に、連続して呼び出されるタスクを生成する。
     */
    public <R> RxTaskBuilder<R> chain(RxTask.AsyncChain<T, R> action) {
        RxTaskBuilder<R> result = new RxTaskBuilder<R>(this, mSubscription)
                .subscribeOn(mThreadTarget)
                .observeOn(mTask.mObserveTarget)
                .async((RxTask<R> chainTask) -> action.call((T) mTask.getResult(), chainTask));

        mTask.mChainTask = result;
        return result;
    }

    public boolean isStartedTask() {
        return mStartedTask;
    }

    /**
     * セットアップを完了し、処理を開始する
     */
    public RxTask<T> start() {
        // 実行準備する
        mTask.mState = RxTask.State.Pending;
        // キャンセルを購読対象と同期させる
        mTask.mSubscribeCancelSignal = (task) -> mSubscription.isCanceled(mTask.mObserveTarget);

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
            mSubscription.getHandler().post(() -> {
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
                mSubscription.add(mTask.mObserveTarget, subscribe);
            });
        }
        return mTask;
    }
}
