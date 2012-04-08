package com.akuroda.hadoop.jrubyexample;
import com.akuroda.hadoop.MapperAdapter;

/**
 * This class shows how to use MapperAdapter
 *
 */
public class CountMapperAdapter extends MapperAdapter {
	public CountMapperAdapter() {
		// pass jruby mapper to MapperAdapter
		setMapper(new CountMapper());
	}
}
