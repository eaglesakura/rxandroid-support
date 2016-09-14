package com.eaglesakura.android.rx;


import com.eaglesakura.android.rx.event.LifecycleEventImpl;

import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import rx.Observable;
import rx.Subscription;
import rx.subjects.BehaviorSubject;
import rx.subscriptions.CompositeSubscription;

/**
 * 実行対象のスレッドと、コールバック対象のスレッドをそれぞれ管理する。
 * <p>
 * Fragment等と関連付けられ、そのライフサイクルを離れると自動的にコールバックを呼びださなくする。
 */
public class PendingCallbackQueue {
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
    private LifecycleState mState = LifecycleState.NewObject;

    private ThreadControllerImpl mThreadController = new ThreadControllerImpl();

    /**
     * 処理をワンテンポ遅らせるためのハンドラ
     */
    private Handler mHandler = new Handler(Looper.getMainLooper());

    private Observable<LifecycleEvent> mObservable;

    public PendingCallbackQueue() {
        for (CallbackTime obs : CallbackTime.values()) {
            mStateControllers.add(new StateController(obs));
        }
    }

    /**
     * 現在認識されているステートを取得する
     */
    public LifecycleState getState() {
        return mState;
    }

    /**
     * ユニットテスト用のハンドラを構築する
     */
    @Deprecated
    public PendingCallbackQueue startUnitTest() {
        HandlerThread handlerThread = new HandlerThread("UnitTestCallback");
        handlerThread.start();
        mHandler = new Handler(handlerThread.getLooper());
        return this;
    }

    public ThreadControllerImpl getThreadController() {
        return mThreadController;
    }

    public Handler getHandler() {
        return mHandler;
    }

    /**
     * 指定したコールバック受付が強制キャンセルならばtrue
     *
     * @param observeTarget 受付先
     */
    @Deprecated
    public boolean isCanceled(ObserveTarget observeTarget) {
        return mStateControllers.get(observeTarget.ordinal()).isCanceled();
    }

    /**
     * 指定したコールバック受付が強制キャンセルならばtrue
     *
     * @param time コールバック対象タイミング
     */
    public boolean isCanceled(CallbackTime time) {
        return mStateControllers.get(time.ordinal()).isCanceled();
    }

    public Observable<LifecycleEvent> getObservable() {
        return mObservable;
    }

    /**
     * ライフサイクルをバインドする
     */
    public PendingCallbackQueue bind(BehaviorSubject<LifecycleEvent> behavior) {
        mObservable = behavior.asObservable();
        mObservable.subscribe(next -> {
            // 継承されたActivityやFragmentはsuper.onの呼び出しで前後が生じるため、統一させるために必ずワンテンポ処理を遅らせる
            mHandler.post(() -> {
                mState = next.getState();

                if (mState == LifecycleState.OnDestroyed) {
                    mThreadController.dispose();
//                    mSubscription.unsubscribe();
                }

                // 保留タスクがあれば流すように促す
                for (StateController ctrl : mStateControllers) {
                    ctrl.onNext();
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

    /**
     * 実行クラスを渡し、処理を行わせる。
     * <p>
     * 実行保留中であれば一旦キューに貯め、resumeのタイミングでキューを全て実行させる。
     */
    @Deprecated
    public void run(ObserveTarget target, Runnable callback) {
        run(target.asCallbackTarget(), callback);
    }

    /**
     * 実行クラスを渡し、処理を行わせる。
     * <p>
     * 実行保留中であれば一旦キューに貯め、resumeのタイミングでキューを全て実行させる。
     */
    public void run(CallbackTime target, Runnable callback) {
        mStateControllers.get(target.ordinal()).run(callback);
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
     * 実行クラスを渡し、実行待ちを行う。
     *
     * デッドロック等の事情によりタイムアウトやフリーズの原因になるので、実行には注意すること。
     *
     * @param time      実行条件
     * @param callback  実行内容
     * @param timeoutMs 待ち時間
     */
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
     * 実行クラスを渡し、実行待ちを行う。
     *
     * デッドロック等の事情によりタイムアウトやフリーズの原因になるので、実行には注意すること。
     *
     * @param target    実行条件
     * @param callback  実行内容
     * @param timeoutMs 待ち時間
     */
    @Deprecated
    public void runWithWait(ObserveTarget target, Runnable callback, long timeoutMs) throws TimeoutException {
        Object lock = new Object();
        run(target, () -> {
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
     * 各ステートを制御する
     */
    class StateController {
        CallbackTime mCallbackTarget;

        List<Runnable> mPendings = new ArrayList<>();

        public StateController(CallbackTime callbackTarget) {
            mCallbackTarget = callbackTarget;
        }

        /**
         * 強制的にキャンセルさせるならばtrue
         */
        boolean isCanceled() {
            if (mCallbackTarget == CallbackTime.FireAndForget) {
                // 打ちっぱなしならキャンセルはしなくて良い
                return false;
            } else if (mCallbackTarget == CallbackTime.CurrentForeground) {
                // resume状態以外はキャンセルとして扱う
                return mState.ordinal() >= LifecycleState.OnPaused.ordinal() || mSubscription.isUnsubscribed();
            } else {
                // それ以外なら購読フラグと連動する
                return mSubscription.isUnsubscribed();
            }
        }

        /**
         * 保留状態であればtrue
         */
        boolean isPending() {
            if (mState == null || mCallbackTarget == CallbackTime.FireAndForget) {
                // ステートが指定されてないか、撃ちっぱなしであれば保留は行わない
                return false;
            }

            switch (mCallbackTarget) {
                // オブジェクトがForegroundにあるなら
                case Foreground:
                case CurrentForeground: {
                    return mState.ordinal() < LifecycleState.OnResumed.ordinal()
                            || mState.ordinal() >= LifecycleState.OnPaused.ordinal();
                }
                // オブジェクトが生きているなら
                case Alive: {
                    return mState.ordinal() < LifecycleState.OnCreated.ordinal()
                            || mState.ordinal() >= LifecycleState.OnDestroyed.ordinal();
                }
                default:
                    // not impl
                    throw new IllegalStateException();
            }
        }

        void onNext() {
            if (mCallbackTarget == CallbackTime.CurrentForeground && mState == LifecycleState.OnResumed) {
                // Pauseを解除されたタイミングで、保留コールバックを全て排除する
                mPendings.clear();
            }

            if (!mPendings.isEmpty() && !isPending()) {
                // 保留から解除されたら、保留されていたタスクを流す
                List<Runnable> executes = new ArrayList<>(mPendings);
                mPendings.clear();
                mHandler.post(() -> {
                    if (mSubscription.isUnsubscribed()) {
                        // 未購読状態になっているので何もしない
                        return;
                    }

                    for (Runnable call : executes) {
                        call.run();
                    }
                });
            }
        }

        /**
         * コールバックを追加する
         */
        void run(Runnable callback) {
            if (isPending()) {
                mPendings.add(callback);
            } else if (Thread.currentThread().equals(mHandler.getLooper().getThread())) {
                callback.run();
            } else {
                mHandler.post(callback);
            }
        }
    }

}
