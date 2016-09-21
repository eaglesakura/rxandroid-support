package com.eaglesakura.android.rx;

/**
 *
 */
public class ExecuteTargetSettings {
    /**
     * CPUコア数
     */
    static int sCpuCoreCount = Math.max(1, Runtime.getRuntime().availableProcessors());

    /**
     * Fragment/Activity等のローカルで扱う並列スレッド数
     */
    static int sLocalParallelsThreads = (sCpuCoreCount * 2);

    /**
     * プロセス共有の最大並列数
     */
    static int sGlobalParallelsThreads = (sCpuCoreCount * 2);

    /**
     * Google-Volleyに合わせて4, それ以上の設定では接続上限に達する恐れがある。
     */
    static int sNetworkThreads = 4;

    public static void setLocalParallelsThreads(int localParallelsThreads) {
        sLocalParallelsThreads = localParallelsThreads;
    }

    public static void setGlobalParallelsThreads(int globalParallelsThreads) {
        sGlobalParallelsThreads = globalParallelsThreads;
    }

    public static void setNetworkThreads(int networkThreads) {
        sNetworkThreads = networkThreads;
    }

    public static int getCpuCoreCount() {
        return sCpuCoreCount;
    }

    public static int getLocalParallelsThreads() {
        return sLocalParallelsThreads;
    }

    public static int getGlobalParallelsThreads() {
        return sGlobalParallelsThreads;
    }

    public static int getNetworkThreads() {
        return sNetworkThreads;
    }
}
