package com.eaglesakura.android.rx;

import com.eaglesakura.android.AndroidSupportTestCase;

import org.robolectric.annotation.Config;

@Config(constants = BuildConfig.class, packageName = BuildConfig.APPLICATION_ID, sdk = 21)
public abstract class UnitTestCase extends AndroidSupportTestCase {
}
