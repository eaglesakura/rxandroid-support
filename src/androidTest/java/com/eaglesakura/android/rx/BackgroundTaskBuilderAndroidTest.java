package com.eaglesakura.android.rx;

import com.eaglesakura.android.devicetest.DeviceTestCase;
import com.eaglesakura.android.rx.error.TaskCanceledException;
import com.eaglesakura.android.rx.event.LifecycleEventImpl;
import com.eaglesakura.android.thread.ui.UIHandler;
import com.eaglesakura.android.util.AndroidThreadUtil;
import com.eaglesakura.thread.Holder;
import com.eaglesakura.util.LogUtil;
import com.eaglesakura.util.Util;

import org.junit.Test;

import rx.subjects.BehaviorSubject;

public class BackgroundTaskBuilderAndroidTest extends DeviceTestCase {

    class LifecycleItem {
        BehaviorSubject<LifecycleEvent> mSubject = BehaviorSubject.create(new LifecycleEventImpl(LifecycleState.NewObject));
        PendingCallbackQueue mCallbackQueue = new PendingCallbackQueue();

        public LifecycleItem() {
            mCallbackQueue.bind(mSubject);
            next(LifecycleState.OnCreated);
            next(LifecycleState.OnStarted);
        }

        public void onCreate() {
            next(LifecycleState.OnCreated);
        }

        public void onResume() {
            next(LifecycleState.OnResumed);
        }

        public void onPause() {
            next(LifecycleState.OnPaused);
        }

        public void onDestroy() {
            next(LifecycleState.OnStopped);
            next(LifecycleState.OnDestroyed);
        }

        void next(LifecycleState state) {
            UIHandler.postUI(() -> {
                mSubject.onNext(new LifecycleEventImpl(state));
            });

            while (state != mCallbackQueue.getState()) {
                Util.sleep(10);
            }
        }
    }

    //    @Test
    public void タスクのメモリリークがないことを確認する() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        try {
            item.onResume();

            for (int i = 0; i < 128; ++i) {
                for (int k = 0; k < 128; ++k) {
                    BackgroundTask rxTask = new BackgroundTaskBuilder<byte[]>(item.mCallbackQueue)
                            .async(new BackgroundTask.Async<byte[]>() {
                                byte[] buffer;

                                @Override
                                public byte[] call(BackgroundTask<byte[]> task) throws Throwable {
                                    buffer = new byte[1024 * 1024];
                                    return buffer;
                                }
                            })
                            .executeOn(ExecuteTarget.LocalQueue)
                            .callbackOn(CallbackTime.FireAndForget)
                            .start();

                    byte[] buffer = (byte[]) rxTask.await();
                    assertNotNull(buffer);
                    assertEquals(buffer.length, 1024 * 1024);
                }
                System.gc();
            }
        } finally {
            item.onPause();
            item.onDestroy();
        }
    }

    @Test
    public void タスクが同期実行できる() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<Boolean>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(100);
                        return true;
                    })
                    .callbackOn(CallbackTime.FireAndForget)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .start();

        } finally {
            item.onPause();
            item.onDestroy();
        }

        assertEquals(rxTask.await(1000 * 5), Boolean.TRUE);
    }

    @Test
    public void ライフサイクル状態によってコールバックが保留される() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        Holder<Boolean> callbackCheck = new Holder<>();
        callbackCheck.set(Boolean.FALSE);
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<Boolean>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(100);
                        return true;
                    })
                    .callbackOn(CallbackTime.Foreground)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .completed((it, task) -> {
                        callbackCheck.set(Boolean.TRUE);
                    }).start();

            item.onPause(); // アプリがバックグラウンドに移った
            rxTask.await(1000); // 処理が終わるまで待つ

            Util.sleep(100);
            assertEquals(callbackCheck.get(), Boolean.FALSE);   // まだコールバックされていない

            item.onResume();    // アプリがフォアグラウンドに復帰した
            Util.sleep(100);

            assertEquals(callbackCheck.get(), Boolean.TRUE);     // resumeされればちゃんとコールバックされる
        } finally {
            item.onPause();
            item.onDestroy();
        }
    }

    @Test
    public void ライフサイクル状態によってタスクが実行される_Alive() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        Holder<Boolean> callbackCheck = new Holder<>();
        callbackCheck.set(Boolean.FALSE);
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(500);
                        return true;
                    })
                    .callbackOn(CallbackTime.Alive)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .completed((it, task) -> {
                        callbackCheck.set(Boolean.TRUE);
                    }).canceled(task -> fail())
                    .start();

            item.onPause(); // アプリがバックグラウンドに移った
            rxTask.await(1000); // 処理が終わるまで待つ
            Util.sleep(100);

            assertEquals(callbackCheck.get(), Boolean.TRUE);     // コールバックが実行されて、setされる。
        } finally {
            item.onPause();
            item.onDestroy();
        }
    }

    @Test
    public void ライフサイクル状態によってタスクがキャンセルさる_Alive() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        Holder<Boolean> callbackCheck = new Holder<>();
        callbackCheck.set(Boolean.FALSE);
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(500);
                        return true;
                    })
                    .callbackOn(CallbackTime.Alive)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .completed((it, task) -> {
                        callbackCheck.set(Boolean.TRUE);
                    }).start();

            item.onPause(); // アプリがバックグラウンドに移った
            assertFalse(rxTask.isCanceled());
            item.onDestroy(); // アプリが廃棄された
            assertTrue(rxTask.isCanceled());
            try {
                rxTask.await(1000); // 処理が終わるまで待つ

                fail(); // アプリがバックグラウンドにあるため、task.waitTimeによってキャンセル例外で死んでいるはずである
            } catch (TaskCanceledException e) {
                LogUtil.log("Task Canceled");
            }
            assertEquals(callbackCheck.get(), Boolean.FALSE);     // コールバックは捨てられるので、呼びだされてはならない。
        } finally {
        }
    }

    @Test
    public void ライフサイクル状態によってタスクが実行される_FireAndForget() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        Holder<Boolean> callbackCheck = new Holder<>();
        callbackCheck.set(Boolean.FALSE);
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(500);
                        return true;
                    })
                    .callbackOn(CallbackTime.FireAndForget)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .completed((it, task) -> {
                        callbackCheck.set(Boolean.TRUE);
                    }).canceled(task -> fail()).start();

            item.onPause(); // アプリがバックグラウンドに移った
            item.onDestroy(); // アプリが廃棄された
            rxTask.await(1000); // 処理が終わるまで待つ
            Util.sleep(100);

            assertEquals(callbackCheck.get(), Boolean.TRUE);     // コールバックが実行されて、setされる。
        } finally {
            item.onPause();
            item.onDestroy();
        }
    }

    @Test
    public void ライフサイクル状態によってタスクがキャンセルされない_FireAndForget() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        Holder<Boolean> callbackCheck = new Holder<>();
        callbackCheck.set(Boolean.FALSE);
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(500);
                        return true;
                    })
                    .callbackOn(CallbackTime.FireAndForget)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .completed((it, task) -> {
                        callbackCheck.set(Boolean.TRUE);
                    }).canceled(task -> fail()).start();

            item.onPause(); // アプリがバックグラウンドに移った
            assertFalse(rxTask.isCanceled());
            item.onDestroy(); // アプリが廃棄された
            assertFalse(rxTask.isCanceled());
            rxTask.await(1000); // 処理が終わるまで待つ
            sleep(100);
            assertEquals(callbackCheck.get(), Boolean.TRUE);     // コールバックが実行されているはずである
        } finally {
        }
    }

    @Test
    public void ライフサイクル状態によってタスクが実行される_Foreground() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        Holder<Boolean> callbackCheck = new Holder<>();
        callbackCheck.set(Boolean.FALSE);
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(500);
                        return true;
                    })
                    .callbackOn(CallbackTime.Foreground)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .completed((it, task) -> {
                        callbackCheck.set(Boolean.TRUE);
                    }).canceled(task -> fail()).start();

            item.onPause(); // アプリがバックグラウンドに移った
            assertFalse(rxTask.isCanceled());
            item.onResume(); // アプリがForegroundに戻った
            assertFalse(rxTask.isCanceled());
            item.onPause(); // アプリがバックグラウンドに移った

            rxTask.await(1000); // 処理が終わるまで待つ

            Util.sleep(100);
            assertEquals(callbackCheck.get(), Boolean.FALSE);   // まだコールバックされていない

            item.onResume();    // アプリがフォアグラウンドに復帰した
            Util.sleep(100);

            assertEquals(callbackCheck.get(), Boolean.TRUE);     // コールバックが実行されて、setされる。
        } finally {
            item.onPause();
            item.onDestroy();
        }
    }

    @Test
    public void ライフサイクル状態によってタスクがキャンセルされる_Foreground() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        Holder<Boolean> callbackCheck = new Holder<>();
        callbackCheck.set(Boolean.FALSE);
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(500);
                        return true;
                    })
                    .callbackOn(CallbackTime.Foreground)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .completed((it, task) -> {
                        fail();
                    }).start();

            item.onPause(); // アプリがバックグラウンドに移った
            assertFalse(rxTask.isCanceled());
            item.onDestroy(); // アプリが廃棄された
            assertTrue(rxTask.isCanceled());

            try {
                rxTask.await(1000); // 処理が終わるまで待つ

                fail(); // アプリがバックグラウンドにあるため、task.waitTimeによってキャンセル例外で死んでいるはずである
            } catch (TaskCanceledException e) {
                LogUtil.log("Task Canceled");
            }
            assertEquals(callbackCheck.get(), Boolean.FALSE);     // コールバックは捨てられるので、呼びだされてはならない。
        } finally {
        }
    }

    @Test
    public void ライフサイクル状態によってタスクが実行される_CurrentForeground() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        Holder<Boolean> callbackCheck = new Holder<>();
        callbackCheck.set(Boolean.FALSE);
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(500);
                        return true;
                    })
                    .callbackOn(CallbackTime.CurrentForeground)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .completed((it, task) -> {
                        callbackCheck.set(Boolean.TRUE);
                    }).canceled(task -> fail()).start();


            rxTask.await(1000); // 処理が終わるまで待つ
            sleep(100);

            assertEquals(callbackCheck.get(), Boolean.TRUE);     // コールバックが実行されて、setされる。
        } finally {
            item.onPause();
            item.onDestroy();
        }
    }

    @Test
    public void ライフサイクル状態によってタスクがキャンセルされる_CurrentForeground() throws Throwable {
        AndroidThreadUtil.assertBackgroundThread();
        assertTrue(isTestingThread());

        LifecycleItem item = new LifecycleItem();
        BackgroundTask rxTask;
        Holder<Boolean> callbackCheck = new Holder<>();
        callbackCheck.set(Boolean.FALSE);
        try {
            item.onResume();

            rxTask = new BackgroundTaskBuilder<>(item.mCallbackQueue)
                    .async(task -> {
                        LogUtil.log("Call Async!");

                        assertFalse(isTestingThread()); // バックグラウンドで実行されている

                        task.waitTime(500);
                        return true;
                    })
                    .callbackOn(CallbackTime.CurrentForeground)
                    .executeOn(ExecuteTarget.LocalQueue)
                    .completed((it, task) -> {
                        fail();
                    }).start();

            item.onPause(); // アプリがバックグラウンドに移った

            // 一度でもStateが変わったらキャンセル扱いである
            assertTrue(rxTask.isCanceled());
            item.onResume(); // アプリがForegroundに戻った
            assertTrue(rxTask.isCanceled());
            item.onPause(); // アプリがバックグラウンドに移った

            try {
                rxTask.await(1000); // 処理が終わるまで待つ

                fail(); // アプリがバックグラウンドにあるため、task.waitTimeによってキャンセル例外で死んでいるはずである
            } catch (TaskCanceledException e) {
                LogUtil.log("Task Canceled");
            }

            item.onResume();    // アプリがフォアグラウンドに復帰した
            Util.sleep(100);

            assertEquals(callbackCheck.get(), Boolean.FALSE);     // コールバックは捨てられるので、呼びだされてはならない。
        } finally {
            item.onPause();
            item.onDestroy();
        }
    }

}
