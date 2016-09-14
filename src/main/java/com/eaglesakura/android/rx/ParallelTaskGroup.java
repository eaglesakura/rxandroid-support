package com.eaglesakura.android.rx;

import android.support.annotation.NonNull;
import android.support.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * 複数タスクをグループ制御する
 */
public class ParallelTaskGroup<T> {

    @NonNull
    final Callback<T> mCallback;

    /**
     * 最大並列タスク
     */
    int mMaxParallelTasks = 3;

    public ParallelTaskGroup(@NonNull Callback<T> callback) {
        mCallback = callback;
    }

    /**
     * 全てのタスクが完了するまで待つ
     *
     * @return 処理したタスク数
     */
    public int await() throws Throwable {
        List<BackgroundTask> tasks = new LinkedList<>();

        int finishedTasks = 0;

        while (true) {
            // 不要なタスクを排除する
            {
                Iterator<BackgroundTask> iterator = tasks.iterator();
                while (iterator.hasNext()) {
                    BackgroundTask task = iterator.next();
                    task.throwIfCanceled();
                    task.throwIfError();

                    // タスクが完了したので排除
                    if (task.isFinished()) {
                        ++finishedTasks;
                        mCallback.onTaskCompleted(this, task);
                        iterator.remove();
                    }
                }
            }

            if (tasks.size() < mMaxParallelTasks) {
                // タスクを生成する
                BackgroundTask<T> task = mCallback.onNextTask(this);
                if (task != null) {
                    tasks.add(task);
                }
            }

            // タスクが空になった
            if (tasks.isEmpty()) {
                return finishedTasks;
            }

            Thread.sleep(1);
        }
    }

    public interface Callback<T> {
        /**
         * 次のタスクを取得する
         */
        @Nullable
        BackgroundTask<T> onNextTask(@NonNull ParallelTaskGroup<T> group) throws Throwable;

        /**
         * タスクが完了した
         */
        void onTaskCompleted(@NonNull ParallelTaskGroup<T> group, @NonNull BackgroundTask task) throws Throwable;
    }
}
