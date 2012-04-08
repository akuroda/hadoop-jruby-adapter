package com.akuroda.hadoop;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Logger;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.jruby.RubyObject;

/**
 * Adapter for forwarding mapreduce Reducer methods to jruby Reducer methods
 */
public class ReducerAdapter extends
	Reducer<Writable, Writable, Writable, Writable> {

	/**
	 * jruby object implements reduce() method 
	 */
	private RubyObject obj = null;
	private Method reduceMethod = null;
	private boolean initialized = false;
	static Logger logger = Logger.getLogger(ReducerAdapter.class.getName());

	/**
	 * the subclass should set jruby reducer object using setReducer() in the constructor
	 */
	public ReducerAdapter() {
	}

	/**
	 * get reduce method from jruby object
	 * @return reduce method of jruby object
	 */
	private Method getReduceMethod() {
		Method method = null;
		Class[] params = new Class[] {Writable.class, Iterable.class, Reducer.Context.class};
		try {
			method = obj.getClass().getMethod("reduce", params);
		} catch (NoSuchMethodException nme) {
			logger.warning("ReducerAdapter: cannot find map method, use default mapper method");
		}
		return method;
	}
	
	/**
	 * set jruby reducer object
	 * @param obj jruby reducer object
	 */
	protected void setReducer(RubyObject obj) {
		this.obj = obj;
	}

	/**
	 * call cleanup method of jruby object or Reducer#cleanup()
	 *  (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	public void cleanup(Reducer.Context context)
		throws IOException, InterruptedException {
		Class[] params = new Class[] {Reducer.Context.class};
		try {
			Method method = obj.getClass().getMethod("cleanup", params);
			if (method != null) {
				method.invoke(obj, new Object[] {context});
			}
		} catch (IllegalAccessException iae) {
			throw new RuntimeException(iae);
		} catch (InvocationTargetException ite) {
			throw new RuntimeException(ite);
		} catch (NoSuchMethodException nme) {
			super.cleanup(context);
		}
	}

	/**
	 * call reduce method of jruby object or Reducer#reduce()
	 *  (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	public void reduce(Writable key, Iterable<Writable> values, Context context)
		throws IOException, InterruptedException {
		if (initialized == false) {
			this.reduceMethod = getReduceMethod();
			initialized = true;
		}
		try {
			if (reduceMethod != null) {
				reduceMethod.invoke(obj, new Object[] {key, values, context});
			} else {
				super.reduce(key, values, context);
			}
		} catch (IllegalAccessException iae) {
			throw new RuntimeException(iae);
		} catch (InvocationTargetException ite) {
			throw new RuntimeException(ite);
		}
	}

	/**
	 * call setup method of jruby object or Reducer#setup()
	 *  (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	public void setup(Reducer.Context context)
		throws IOException, InterruptedException {
		Class[] params = new Class[] {Reducer.Context.class};
		try {
			Method method = obj.getClass().getMethod("cleanup", params);
			if (method != null) {
				method.invoke(obj, new Object[] {context});
			}
		} catch (IllegalAccessException iae) {
			throw new RuntimeException(iae);
		} catch (InvocationTargetException ite) {
			throw new RuntimeException(ite);
		} catch (NoSuchMethodException nme) {
			super.setup(context);
		}
	}
}
