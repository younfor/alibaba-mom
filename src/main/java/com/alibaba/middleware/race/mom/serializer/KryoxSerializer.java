package com.alibaba.middleware.race.mom.serializer;

import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;

import sun.reflect.ReflectionFactory;

import com.esotericsoftware.kryo.Kryo;

public class KryoxSerializer extends Kryo {

	private final ReflectionFactory REFLECTION_FACTORY = ReflectionFactory
			.getReflectionFactory();

	private final ConcurrentHashMap<Class<?>, Constructor<?>> _constructors = new ConcurrentHashMap<Class<?>, Constructor<?>>();

	@Override
	public <T> T newInstance(Class<T> type) {
		try {
			return super.newInstance(type);
		} catch (Exception e) {
			return (T) newInstanceFromReflectionFactory(type);
		}
	}

	private Object newInstanceFrom(Constructor<?> constructor) {
		try {
			return constructor.newInstance();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T newInstanceFromReflectionFactory(Class<T> type) {
		Constructor<?> constructor = _constructors.get(type);
		if (constructor == null) {
			constructor = newConstructorForSerialization(type);
			Constructor<?> saved = _constructors.putIfAbsent(type, constructor);
			if(saved!=null)
				constructor=saved;
		}
		return (T) newInstanceFrom(constructor);
	}

	private <T> Constructor<?> newConstructorForSerialization(
			Class<T> type) {
		try {
			Constructor<?> constructor = REFLECTION_FACTORY
					.newConstructorForSerialization(type,
							Object.class.getDeclaredConstructor());
			constructor.setAccessible(true);
			return constructor;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
