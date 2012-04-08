package com.akuroda.hadoop;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.io.IOException;
import java.util.logging.Logger;
import org.jruby.RubyObject;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Writable;

/**
 * Adapter for forwarding mapreduce Mapper methods to jruby Mapper methods
 */
public class MapperAdapter extends
	Mapper<Writable, Writable, Writable, Writable> {

	/**
	 * jruby object implements map() method
	 */
	protected RubyObject obj = null;
	protected Method mapMethod = null;
	protected boolean initialized = false;
	static Logger logger = Logger.getLogger(MapperAdapter.class.getName());
	
	/**
	 * the subclass should set jruby mapper object using setMapper() in the constructor
	 */
	public MapperAdapter() {
	}
	
	/**
	 * get map method from jruby object
	 * @return map method of jruby object
	 */
	private Method getMapMethod() {
		Method method = null;
		Class[] params = new Class[] {Writable.class, Writable.class, Mapper.Context.class};
		try {
			method = obj.getClass().getMethod("map", params);
		} catch (NoSuchMethodException nme) {
			logger.warning("MapperAdapter: cannot find map method, use default mapper method");
		}
		return method;
	}
	
	/**
	 * set jruby reducer object
	 * @param obj jruby Mapper object
	 */
	protected void setMapper(RubyObject obj) {
		this.obj = obj;
	}

	/**
	 * call cleanup method of jruby object or Mapper#cleanup()
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void cleanup(Mapper.Context context)
		throws IOException, InterruptedException {
		Class[] params = new Class[] {Mapper.Context.class};
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
	 * call map method of jruby object or Mapper#map()
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(Writable key, Writable value, Mapper.Context context)
		throws IOException, InterruptedException {
		if (initialized == false) {
			this.mapMethod = getMapMethod();
			initialized = true;
		}
		try {
			if (mapMethod != null) {
				mapMethod.invoke(obj, new Object[] {key, value, context});
			} else {
				super.map(key, value, context);
			}
		} catch (InvocationTargetException ite) {
			ite.getCause().printStackTrace();
			throw new RuntimeException(ite);
		} catch (IllegalAccessException iae) {
			throw new RuntimeException(iae);
		}
	}

	/**
	 * call setup method of jruby object or Mapper#setup()
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void setup(Mapper.Context context)
		throws IOException, InterruptedException {
		Class[] params = new Class[] {Mapper.Context.class};
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
