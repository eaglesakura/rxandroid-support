package com.eaglesakura.android.rx;

import java.util.ArrayList;
import java.util.List;

/**
 * ステート管理
 */
public abstract class StateController {

    List<PendingCallbackQueue.PendingTask> mPendingActions = new ArrayList<>();

    StateController() {
    }

    /**
     * 強制的にキャンセルさせるならばtrue
     */
    abstract boolean isCanceled(PendingCallbackQueue current, PendingCallbackQueue.State taskState);

    /**
     * 保留状態であればtrue
     */
    abstract boolean isPending(PendingCallbackQueue current, PendingCallbackQueue.State taskState);

    /**
     * 処理を実行する
     */
    void onNext(PendingCallbackQueue current) {
        // 保留から解除されたら、保留されていたタスクを流す
        synchronized (mPendingActions) {
            if (mPendingActions.isEmpty()) {
                return;
            }

            List<PendingCallbackQueue.PendingTask> executes = new ArrayList<>(mPendingActions);

            for (PendingCallbackQueue.PendingTask task : executes) {
                if (isCanceled(current, task.getDumpState())) {
                    mPendingActions.remove(task);
                } else if (!isPending(current, task.getDumpState())) {
                    task.run();
                    mPendingActions.remove(task);
                }
            }
        }
    }

    /**
     * タスクを実行させる
     *
     * @param current 現在のステート
     * @param task    実行対象のタスク
     */
    void run(PendingCallbackQueue current, PendingCallbackQueue.PendingTask task) {
        synchronized (mPendingActions) {
            if (isCanceled(current, task.getDumpState())) {
                return;
            } else if (isPending(current, task.getDumpState())) {
                mPendingActions.add(task);
                return;
            }

            if (Thread.currentThread().equals(PendingCallbackQueue.sHandler.getLooper().getThread())) {
                task.run();
            } else {
                PendingCallbackQueue.sHandler.post(() -> task.run());
            }
        }
    }

    /**
     * onResume - onPauseの間のみコールバックを受け付ける
     */
    static class ForegroundController extends StateController {

        @Override
        boolean isCanceled(PendingCallbackQueue current, PendingCallbackQueue.State taskState) {
            PendingCallbackQueue.State currentState = current.getCurrentState();
            return currentState.isDestroyed();
        }

        @Override
        boolean isPending(PendingCallbackQueue current, PendingCallbackQueue.State taskState) {
            PendingCallbackQueue.State currentState = current.getCurrentState();
            return !currentState.isForeground();
        }
    }

    /**
     * 現在の前面処理のみコールバックを受け付ける。
     * 現在Backgroundにある場合、タスクは実行されずにキャンセルされる
     *
     * onPauseのタイミングで、保留されているキューは全て廃棄される。
     */
    static class CurrentForegroundController extends StateController {
        @Override
        boolean isCanceled(PendingCallbackQueue current, PendingCallbackQueue.State taskState) {
            // Stateが一度でも切り替わったら全部キャンセルである
            PendingCallbackQueue.State currentState = current.getCurrentState();
            return !currentState.equals(taskState);
        }

        @Override
        boolean isPending(PendingCallbackQueue current, PendingCallbackQueue.State taskState) {
            // 保留は存在しない
            return false;
        }
    }

    /**
     * onCreate - onDestroyの間のみコールバックを受け付ける
     */
    static class AliveController extends StateController {
        @Override
        boolean isCanceled(PendingCallbackQueue current, PendingCallbackQueue.State taskState) {
            PendingCallbackQueue.State currentState = current.getCurrentState();
            return currentState.isDestroyed();
        }

        @Override
        boolean isPending(PendingCallbackQueue current, PendingCallbackQueue.State taskState) {
            PendingCallbackQueue.State currentState = current.getCurrentState();
            return !currentState.isCreated();
        }
    }

    /**
     * 常にコールバックを受け付ける
     */
    static class FireAndForgetController extends StateController {
        @Override
        boolean isCanceled(PendingCallbackQueue current, PendingCallbackQueue.State taskState) {
            return false;
        }

        @Override
        boolean isPending(PendingCallbackQueue current, PendingCallbackQueue.State taskState) {
            return false;
        }
    }
}
