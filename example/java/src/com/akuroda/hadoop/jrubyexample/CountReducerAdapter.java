package com.akuroda.hadoop.jrubyexample;

import com.akuroda.hadoop.ReducerAdapter;

/**
 * This class shows how to use ReducerAdapter
 *
 */
public class CountReducerAdapter extends ReducerAdapter {
	public CountReducerAdapter() {
		// pass jruby reducer to ReducerAdapter
		setReducer(new CountReducer());
	}
}
