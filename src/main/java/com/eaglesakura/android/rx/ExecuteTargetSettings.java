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
     * Google-Volleyが4を基本にしているが、端末の高性能化を考えるとそれよりも多くて問題ないはず。
     */
    static int sNetworkThreads = 6;

    public static void setLocalParallelsThreads(int localParallelsThreads) {
        sLocalParallelsThreads = localParallelsThreads;
    }

    public static void setGlobalParallelsThreads(int globalParallelsThreads) {
        sGlobalParallelsThreads = globalParallelsThreads;
    }

    public static void setNetworkThreads(int networkThreads) {
        sNetworkThreads = networkThreads;
    }
}
