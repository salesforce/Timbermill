package com.datorama.timbermill;

import java.util.HashMap;
import java.util.Map;

public final class Params<T> {
	private Map<String, T> internalMap = new HashMap<>();

	private Params(){}

	public static Params<String> createStringParams(){
		return new Params<>();
	}

	public static Params<Number> createNumberParams(){
		return new Params<>();
	}

	public static Params<Object> createObjectParams(){
		return new Params<>();
	}

	public static <T> Params<T> newParams(){
		return new Params<>();
	}

	public Params<T> put(String key, T value){
		internalMap.put(key, value);
		return this;
	}

	Map<String, T> getMap() {
		return internalMap;
	}
}
