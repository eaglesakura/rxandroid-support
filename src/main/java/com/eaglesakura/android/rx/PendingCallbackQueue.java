package com.eaglesakura.android.rx;


import com.eaglesakura.android.rx.error.TaskCanceledException;
import com.eaglesakura.android.rx.event.LifecycleEventImpl;

import android.os.Handler;
import android.os.Looper;
import android.support.annotation.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.subjects.BehaviorSubject;
import rx.subscriptions.CompositeSubscription;

/**
 * 実行対象のスレッドと、コールバック対象のスレッドをそれぞれ管理する。
 * <p>
 * Fragment等と関連付けられ、そのライフサイクルを離れると自動的にコールバックを呼びださなくする。
 */
public class PendingCallbackQueue {

    /**
     * 処理をワンテンポ遅らせるためのハンドラ
     */
    static Handler sHandler = new Handler(Looper.getMainLooper());


    /**
     * 通常処理されるSubscription
     */
    private CompositeSubscription mSubscription = new CompositeSubscription();

    /**
     * 各Observeステートごとの保留タスク管理
     */
    private List<StateController> mStateControllers = new ArrayList<>();

    /**
     * 初期ステートはNew
     */
    private State mState = new State(LifecycleState.NewObject, 0);

    private ThreadControllerImpl mThreadController = new ThreadControllerImpl();

    private Observable<LifecycleEvent> mObservable;

    public PendingCallbackQueue() {
        for (CallbackTime callbackTime : CallbackTime.values()) {
            mStateControllers.add(callbackTime.newStateController());
        }
    }

    /**
     * 現在認識されているステートを取得する
     */
    @Deprecated
    public LifecycleState getState() {
        return mState.getState();
    }

    /**
     * 現在の状態を取得する
     */
    @NonNull
    public State getCurrentState() {
        return mState;
    }

    public ThreadControllerImpl getThreadController() {
        return mThreadController;
    }

    public Observable<LifecycleEvent> getObservable() {
        return mObservable;
    }

    public Subscription subscribe(Action1<? super LifecycleEvent> onNext) {
        return mObservable.subscribe(onNext);
    }

    /**
     * ライフサイクルをバインドする
     */
    public PendingCallbackQueue bind(BehaviorSubject<LifecycleEvent> behavior) {
        mObservable = behavior.asObservable();
        mObservable.subscribe(next -> {
            // 継承されたActivityやFragmentはsuper.onの呼び出しで前後が生じるため、統一させるために必ずワンテンポ処理を遅らせる
            sHandler.post(() -> {
                mState = mState.nextState(next.getState());

                if (mState.getState() == LifecycleState.OnDestroyed) {
                    mThreadController.dispose();
                    mSubscription.unsubscribe();
                }

                // 保留タスクがあれば流すように促す
                for (StateController ctrl : mStateControllers) {
                    ctrl.onNext(this);
                }
            });
        });
        return this;
    }

    PendingCallbackQueue add(CallbackTime time, Subscription s) {
        if (time != CallbackTime.FireAndForget) {
            mSubscription.add(s);
        }
        return this;
    }

    PendingCallbackQueue remove(Subscription s) {
        mSubscription.remove(s);
        return this;
    }

    private StateController getController(CallbackTime time) {
        return mStateControllers.get(time.ordinal());
    }

    /**
     * @param time      コールしたいタイミング
     * @param dumpState タスク開始時のステート
     */
    public boolean isCanceled(CallbackTime time, State dumpState) {
        return getController(time).isCanceled(this, dumpState);
    }

    /**
     * 実行クラスを渡し、処理を行わせる。
     * <p>
     * 実行保留中であれば一旦キューに貯め、resumeのタイミングでキューを全て実行させる。
     */
    public void run(CallbackTime target, Runnable callback) {
        getController(target).run(this, new PendingTask(callback, mState.dump()));
    }

    /**
     * UnitTest用の空のコントローラを生成する
     */
    public static PendingCallbackQueue newUnitTestController() {
        BehaviorSubject<LifecycleEvent> behavior = BehaviorSubject.create(new LifecycleEventImpl(LifecycleState.NewObject));
        behavior.onNext(new LifecycleEventImpl(LifecycleState.OnResumed));

        PendingCallbackQueue controller = new PendingCallbackQueue();
        controller.bind(behavior);
        return controller;
    }

    /**
     * デフォルト構成で生成する
     *
     * 状態はNewObjectとなる
     */
    public static PendingCallbackQueue newInstance() {
        BehaviorSubject<LifecycleEvent> behavior = BehaviorSubject.create(new LifecycleEventImpl(LifecycleState.NewObject));

        PendingCallbackQueue controller = new PendingCallbackQueue();
        controller.bind(behavior);
        return controller;
    }

    /**
     * 実行クラスを渡し、実行待ちを行う。
     *
     * デッドロック等の事情によりタイムアウトやフリーズの原因になるので、実行には注意すること。
     *
     * @param time      実行条件
     * @param callback  実行内容
     * @param timeoutMs 待ち時間
     */
    @Deprecated
    public void runWithWait(CallbackTime time, Runnable callback, long timeoutMs) throws TimeoutException {
        Object lock = new Object();
        run(time, () -> {
            try {
                callback.run();
            } finally {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        });
        synchronized (lock) {
            try {
                lock.wait(timeoutMs);
            } catch (Exception e) {
            }
        }
    }

    /**
     * キャンセルコールバックを指定して実行待ちを行う
     */
    public void runWithWait(CallbackTime callbackTime, Runnable callback, @NonNull CancelCallback cancelCallback) throws TaskCanceledException {
        Object[] holder = new Object[1];
        run(callbackTime, () -> {
            try {
                callback.run();
            } finally {
                holder[0] = new Object();
            }
        });

        while (holder[0] == null) {
            try {
                if (cancelCallback.isCanceled()) {
                    throw new TaskCanceledException();
                }
            } catch (Throwable e) {
                throw new TaskCanceledException(e);
            }
        }
    }

    public interface CancelCallback {
        boolean isCanceled() throws Throwable;
    }

    public static class PendingTask {
        /**
         * タスク本体
         */
        Runnable mAction;

        /**
         * タスクが発行されたタイミングでのステート
         */
        State mDumpState;

        public PendingTask(Runnable action, State dumpState) {
            mAction = action;
            mDumpState = dumpState;
        }

        public void run() {
            mAction.run();
        }

        public State getDumpState() {
            return mDumpState;
        }
    }


    /**
     * 現在のステートを管理する
     */
    public static class State {
        /**
         * 現在のステートを管理する
         */
        private final LifecycleState mState;

        /**
         * ステート変更番号
         * 必ずインクリメントされる
         */
        private final int mStateChangeCount;

        State(LifecycleState state, int changeNumber) {
            mState = state;
            mStateChangeCount = changeNumber;
        }

        public LifecycleState getState() {
            return mState;
        }

        public int getStateChangeCount() {
            return mStateChangeCount;
        }

        /**
         * オブジェクトが作成状態であればtrue
         */
        public boolean isCreated() {
            return mState.ordinal() >= LifecycleState.OnCreated.ordinal();
        }

        /**
         * Foreground状態であればtrue
         */
        public boolean isForeground() {
            return mState == LifecycleState.OnResumed;
        }

        /**
         * オブジェクトが廃棄状態であればtrue
         */
        public boolean isDestroyed() {
            return mState.ordinal() >= LifecycleState.OnDestroyed.ordinal();
        }

        public synchronized State nextState(LifecycleState nextLifecycle) {
            return new State(nextLifecycle, mStateChangeCount + 1);
        }

        public State dump() {
//            return new State(mState, mStateChangeCount);
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            State state = (State) o;

            if (mStateChangeCount != state.mStateChangeCount) return false;
            return mState == state.mState;

        }

        @Override
        public int hashCode() {
            int result = mState != null ? mState.hashCode() : 0;
            result = 31 * result + mStateChangeCount;
            return result;
        }
    }
}
