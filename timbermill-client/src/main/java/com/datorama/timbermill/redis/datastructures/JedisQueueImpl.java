package com.datorama.timbermill.redis.datastructures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.support.collections.DefaultRedisList;
import org.springframework.data.redis.support.collections.RedisList;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public final class JedisQueueImpl<E> implements BlockingQueue<E> {

	private static final Logger LOG = LoggerFactory.getLogger(JedisQueueImpl.class);

	private final RedisList<E> remoteQueue;
	private final String name;

	public JedisQueueImpl(String name, RedisOperations<String, String> redisTemplate) {
		this.name = name;
		remoteQueue = new DefaultRedisList(this.name, redisTemplate);
	}

	public String getName() {
		return name;
	}

	@Override
	public void clear() {
		remoteQueue.clear();
	}

	//This method has very poor performance since it will take O(N) in the worst case
	@Override
	public boolean contains(Object o) {
		return remoteQueue.contains(o);
	}

	@Override public int drainTo(Collection<? super E> c) {
		return drainTo(c, remoteQueue.size());
	}

	@Override public int drainTo(Collection<? super E> c, int maxElements) {
		int elementsToGet = Math.min(remoteQueue.size(), maxElements);
		c.addAll(remoteQueue.range(0, elementsToGet - 1));
		remoteQueue.trim(elementsToGet, -1);
		return c.size();
	}

	@Override public Iterator<E> iterator() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override public Object[] toArray() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	public boolean isEmpty() {
		return remoteQueue.isEmpty();
	}

	@Override public boolean add(E e) {
		return offer(e);
	}

	@Override public boolean remove(Object o) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override public boolean addAll(Collection<? extends E> c) {
		return offerAll(c);
	}

	@Override public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public boolean offer(E e) {
		return remoteQueue.offer(e);
	}

	@Override public E remove() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	public boolean offerAll(Collection<? extends E> c) {
		return remoteQueue.addAll(remoteQueue.size(), c);
	}

	public E poll() {
		return remoteQueue.poll();
	}

	@Override public E element() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override public E peek() {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		return remoteQueue.poll(timeout, unit);
	}

	@Override public int remainingCapacity() {
		return 0;
	}

	@Override
	public int size() {
		return remoteQueue.size();
	}

	public void destroy() {
		remoteQueue.clear();
	}

	@Override
	public void put(E e) {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		throw new UnsupportedOperationException("Operation not supported");
	}

	@Override public E take() {
		throw new UnsupportedOperationException("Operation not supported");
	}
}
