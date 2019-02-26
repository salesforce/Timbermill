package com.datorama.timbermill;

@FunctionalInterface
public interface CallableTask<V> {

	/**
	 * Computes a result, or throws an exception if unable to do so.
	 *
	 * @return computed result
	 */
	V call();
}
