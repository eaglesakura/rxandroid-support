package com.eaglesakura.android.rx.event;

import com.eaglesakura.android.rx.LifecycleEvent;
import com.eaglesakura.android.rx.LifecycleState;

import android.os.Bundle;
import android.os.PersistableBundle;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;

public class OnSaveEvent implements LifecycleEvent {
    @NonNull
    final Bundle mBundle;

    public OnSaveEvent(Bundle bundle) {
        mBundle = bundle;
    }

    @Override
    public LifecycleState getState() {
        return LifecycleState.OnSaveInstanceState;
    }

    @NonNull
    public Bundle getBundle() {
        return mBundle;
    }
}
