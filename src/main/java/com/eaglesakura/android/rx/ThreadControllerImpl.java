package com.eaglesakura.android.rx;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import rx.Scheduler;
import rx.android.schedulers.AndroidSchedulers;
import rx.schedulers.Schedulers;

/**
 * RxAndroidの実行スレッド制御を行う
 */
class ThreadControllerImpl {

    List<ThreadItem> mThreads = new ArrayList<>();


    /**
     * プロセス共有シリアル
     */
    private static final ThreadItem sGlobalPipeline = new ThreadItem(ExecuteTarget.GlobalQueue);

    /**
     * プロセス共有Parallels
     */
    private static final ThreadItem sGlobalParallels = new ThreadItem(ExecuteTarget.GlobalParallel);

    /**
     * プロセス共有ネットワーク
     */
    private static final ThreadItem sNetworks = new ThreadItem(ExecuteTarget.Network);

    public ThreadControllerImpl() {
        mThreads.add(new ThreadItem(ExecuteTarget.LocalQueue));
        mThreads.add(new ThreadItem(ExecuteTarget.LocalParallel));
        mThreads.add(sGlobalPipeline);
        mThreads.add(sGlobalParallels);
        mThreads.add(sNetworks);
    }

    /**
     * 処理対象のスケジューラを取得する
     */
    @Deprecated
    Scheduler getScheduler(SubscribeTarget target) {
        if (target == SubscribeTarget.NewThread) {
            return Schedulers.newThread();
        } else if (target == SubscribeTarget.MainThread) {
            return AndroidSchedulers.mainThread();
        } else {
            return mThreads.get(target.ordinal()).getScheduler();
        }
    }


    /**
     * 処理対象のスケジューラを取得する
     *
     * MEMO : スケジューラの実際のnew処理はこの呼出まで遅延される
     */
    Scheduler getScheduler(ExecuteTarget target) {
        if (target == ExecuteTarget.NewThread) {
            return Schedulers.newThread();
        } else if (target == ExecuteTarget.MainThread) {
            return AndroidSchedulers.mainThread();
        } else {
            return mThreads.get(target.ordinal()).getScheduler();
        }
    }

    /**
     * 全てのスケジューラを開放する
     */
    public void dispose() {
        for (ThreadItem item : mThreads) {
            item.dispose();
        }
    }

    static class ThreadItem {
        ThreadPoolExecutor mExecutor;
        Scheduler mScheduler;
        ExecuteTarget mTarget;

        public ThreadItem(ExecuteTarget target) {
            this.mTarget = target;
        }

        public Scheduler getScheduler() {
            synchronized (ThreadControllerImpl.class) {
                if (mScheduler == null) {
                    mExecutor = new ThreadPoolExecutor(0, mTarget.getThreadPoolNum(), mTarget.getKeepAliveMs(), TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
                        @Override
                        public void execute(Runnable command) {
                            try {
                                setCorePoolSize(mTarget.getThreadPoolNum());
                                super.execute(command);
                            } finally {
                                setCorePoolSize(0);
                            }
                        }
                    };
                    mScheduler = Schedulers.from(mExecutor);
                } else {
                    mExecutor.setCorePoolSize(mTarget.getThreadPoolNum());
                }
                return mScheduler;
            }
        }

        public void dispose() {
            synchronized (ThreadControllerImpl.class) {
                if (mExecutor != null) {
//                    if (mTarget == ExecuteTarget.LocalQueue || mTarget == ExecuteTarget.LocalParallel) {
//                        // ローカルは完全廃棄する
//                        mExecutor.shutdown();
//                        mExecutor.setCorePoolSize(0);
//                    } else {
//                        // スレッドプールを廃棄する
//                        if (mExecutor.getQueue().isEmpty()) {
//                            mExecutor.setCorePoolSize(0);
//                        }
//                    }
                }
            }
        }
    }
}
