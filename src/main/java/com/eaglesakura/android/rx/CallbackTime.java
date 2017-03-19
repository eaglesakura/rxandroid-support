package com.eaglesakura.android.rx;

/**
 * 非同期処理のコールバック待ちを行う場所
 */
public enum CallbackTime {
    /**
     * onResume - onPauseの間のみコールバックを受け付ける
     */
    Foreground {
        @Override
        StateController newStateController() {
            return new StateController.ForegroundController();
        }
    },

    /**
     * 現在の前面処理のみコールバックを受け付ける。
     *
     * onPauseのタイミングで、保留されているキューは全て廃棄される。
     */
    CurrentForeground {
        @Override
        StateController newStateController() {
            return new StateController.CurrentForegroundController();
        }
    },

    /**
     * onCreate - onDestroyの間のみコールバックを受け付ける
     */
    Alive {
        @Override
        StateController newStateController() {
            return new StateController.AliveController();
        }
    },

    /**
     * 常にコールバックを受け付ける
     */
    FireAndForget {
        @Override
        StateController newStateController() {
            return new StateController.FireAndForgetController();
        }
    };

    abstract StateController newStateController();
}
