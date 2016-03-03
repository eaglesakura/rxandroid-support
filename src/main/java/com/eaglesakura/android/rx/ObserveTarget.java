package com.eaglesakura.android.rx;

/**
 * 非同期処理のコールバック待ちを行う場所
 */
public enum ObserveTarget {
    /**
     * onResume - onPauseの間のみコールバックを受け付ける
     */
    Foreground,

    /**
     * 現在の前面処理のみコールバックを受け付ける。
     *
     * onPauseのタイミングで、保留されているキューは全て廃棄される。
     */
    CurrentForeground,

    /**
     * onCreate - onDestroyの間のみコールバックを受け付ける
     */
    Alive,

    /**
     * 常にコールバックを受け付ける
     */
    FireAndForget,
}
