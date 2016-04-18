package com.eaglesakura.android.rx;


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
public class SubscriptionController {
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

    private ThreadController mThreadController = new ThreadController();

    /**
     * 処理をワンテンポ遅らせるためのハンドラ
     */
    private Handler mHandler = new Handler(Looper.getMainLooper());

    private Observable<LifecycleState> mObservable;

    public SubscriptionController() {
        for (ObserveTarget obs : ObserveTarget.values()) {
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
    public SubscriptionController startUnitTest() {
        HandlerThread handlerThread = new HandlerThread("UnitTestCallback");
        handlerThread.start();
        mHandler = new Handler(handlerThread.getLooper());
        return this;
    }

    public ThreadController getThreadController() {
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
    public boolean isCanceled(ObserveTarget observeTarget) {
        return mStateControllers.get(observeTarget.ordinal()).isCanceled();
    }

    public Observable<LifecycleState> getObservable() {
        return mObservable;
    }

    /**
     * ライフサイクルをバインドする
     */
    public SubscriptionController bind(BehaviorSubject<LifecycleState> behavior) {
        mObservable = behavior.asObservable();
        mObservable.subscribe(next -> {
            // 継承されたActivityやFragmentはsuper.onの呼び出しで前後が生じるため、統一させるために必ずワンテンポ処理を遅らせる
            mHandler.post(() -> {
                mState = next;

                if (next == LifecycleState.OnDestroyed) {
                    mThreadController.dispose();
                    mSubscription.unsubscribe();
                }

                // 保留タスクがあれば流すように促す
                for (StateController ctrl : mStateControllers) {
                    ctrl.onNext();
                }
            });
        });
        return this;
    }

    SubscriptionController add(ObserveTarget target, Subscription s) {
        if (target != ObserveTarget.FireAndForget) {
            mSubscription.add(s);
        }
        return this;
    }

    /**
     * 実行クラスを渡し、処理を行わせる。
     * <p>
     * 実行保留中であれば一旦キューに貯め、resumeのタイミングでキューを全て実行させる。
     */
    public void run(ObserveTarget target, Runnable callback) {
        mStateControllers.get(target.ordinal()).run(callback);
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
        ObserveTarget mCallbackTarget;

        List<Runnable> mPendings = new ArrayList<>();

        public StateController(ObserveTarget callbackTarget) {
            mCallbackTarget = callbackTarget;
        }

        /**
         * 強制的にキャンセルさせるならばtrue
         */
        boolean isCanceled() {
            if (mCallbackTarget == ObserveTarget.FireAndForget) {
                // 打ちっぱなしならキャンセルはしなくて良い
                return false;
            } else if (mCallbackTarget == ObserveTarget.CurrentForeground) {
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
            if (mState == null || mCallbackTarget == ObserveTarget.FireAndForget) {
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
            if (mCallbackTarget == ObserveTarget.CurrentForeground && mState == LifecycleState.OnResumed) {
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
