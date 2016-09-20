package com.eaglesakura.android.rx;

/**
 * 実行スレッド選定
 */
@Deprecated
public enum SubscribeTarget {
    /**
     * 直列化されたパイプラインで制御する
     */
    Pipeline {
        @Override
        ExecuteTarget asExecuteTarget() {
            return ExecuteTarget.LocalQueue;
        }
    },

    /**
     * 並列化されたスレッドプールで制御する
     */
    Parallels {
        @Override
        ExecuteTarget asExecuteTarget() {
            return ExecuteTarget.LocalParallel;
        }
    },

    /**
     * プロセス内で共有される直列化された処理
     */
    GlobalPipeline {
        @Override
        ExecuteTarget asExecuteTarget() {
            return ExecuteTarget.GlobalQueue;
        }
    },

    /**
     * プロセス内で共有される並列化された処理
     */
    GlobalParallels {
        @Override
        ExecuteTarget asExecuteTarget() {
            return ExecuteTarget.GlobalParallel;
        }
    },

    /**
     * ネットワーク処理用スレッド
     *
     * これはグローバルで共有され、適度なスレッド数に保たれる
     */
    Network {
        @Override
        ExecuteTarget asExecuteTarget() {
            return ExecuteTarget.Network;
        }
    },

    /**
     * 専用スレッドを生成する
     */
    NewThread {
        @Override
        ExecuteTarget asExecuteTarget() {
            return ExecuteTarget.NewThread;
        }
    },

    /**
     * UiThreadで処理する
     */
    MainThread {
        @Override
        ExecuteTarget asExecuteTarget() {
            return ExecuteTarget.MainThread;
        }
    };

    @Deprecated
    abstract ExecuteTarget asExecuteTarget();
}
