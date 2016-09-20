package com.eaglesakura.android.rx;

/**
 * 実行スレッド選定
 */
public enum ExecuteTarget {
    /**
     * 直列化されたパイプラインで制御する
     */
    LocalQueue {
        @Override
        int getKeepAliveMs() {
            return 1000 * 5;
        }

        @Override
        int getThreadPoolNum() {
            return 1;
        }
    },

    /**
     * 並列化されたスレッドプールで制御する
     */
    LocalParallel {
        @Override
        int getKeepAliveMs() {
            return 1000 * 5;
        }

        @Override
        int getThreadPoolNum() {
            return ExecuteTargetSettings.sLocalParallelsThreads;
        }
    },

    /**
     * プロセス内で共有される直列化された処理
     */
    GlobalQueue {
        @Override
        int getKeepAliveMs() {
            return 1000 * 3;
        }

        @Override
        int getThreadPoolNum() {
            return 1;
        }
    },

    /**
     * プロセス内で共有される並列化された処理
     */
    GlobalParallel {
        @Override
        int getKeepAliveMs() {
            return 1000 * 3;
        }

        @Override
        int getThreadPoolNum() {
            return ExecuteTargetSettings.sGlobalParallelsThreads;
        }
    },

    /**
     * ネットワーク処理用スレッド
     *
     * これはグローバルで共有され、適度なスレッド数に保たれる
     */
    Network {
        @Override
        int getKeepAliveMs() {
            return 500;
        }

        @Override
        int getThreadPoolNum() {
            return ExecuteTargetSettings.sNetworkThreads;
        }
    },

    /**
     * 専用スレッドを生成する
     */
    NewThread {
        @Override
        int getKeepAliveMs() {
            return 0;
        }

        @Override
        int getThreadPoolNum() {
            return 0;
        }
    },

    /**
     * UiThreadで処理する
     */
    MainThread {
        @Override
        int getKeepAliveMs() {
            return 0;
        }

        @Override
        int getThreadPoolNum() {
            return 0;
        }
    };

    /**
     * スレッドキャッシュ時間を取得する
     */
    abstract int getKeepAliveMs();

    /**
     * 最大スレッド数を取得する
     */
    abstract int getThreadPoolNum();
}
