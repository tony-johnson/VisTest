package org.lsst.ccs.visualization.server;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple blocking map. Only get, put and remove are supported.
 *
 * @author tonyj
 */
class BlockingMap<K, V> {

    private final ConcurrentMap<K, Latch<V>> map = new ConcurrentHashMap<>();
    private final Lock lock = new ReentrantLock();

    BlockingMap() {

    }

    void put(K key, V value) {
        lock.lock();
        try {
            Latch<V> latch = map.get(key);
            if (latch == null) {
                map.put(key, new Latch<>(value));
            } else {
                latch.put(value);
            }
        } finally {
            lock.unlock();
        }
    }

    V get(K key, int timeout, TimeUnit unit) throws InterruptedException {
        Latch<V> latch = map.get(key);
        if (latch == null) {
            lock.lock();
            try {
                latch = map.get(key);
                if (latch == null) {
                    latch = new Latch<>();
                    map.put(key, latch);
                }
            } finally {
                lock.unlock();
            }
        }
        return latch.get(timeout, unit);
    }

    void remove(K key) {
        // TODO: Should we distinguish a key which was explictly removed, from
        // a get which was never created? Perhaps get should always return null
        // without waiting in the former case.
        lock.lock();
        try {
            map.remove(key);
        } finally {
            lock.unlock();
        }
    }
    
    Set<Map.Entry<K,V>> entrySet() {
        Set<Map.Entry<K,V>> result = new HashSet<>();
        for (Map.Entry<K,Latch<V>> entry : map.entrySet()) {
            V value = entry.getValue().getImmediate();
            if (value != null) {
                result.add(new SimpleImmutableEntry<>(entry.getKey(),value));
            }
        }
        return Collections.unmodifiableSet(result);
    }

    private static class Latch<V> {

        private final CountDownLatch count;
        private V value;

        Latch() {
            this.value = null;
            count = new CountDownLatch(1);
        }

        Latch(V value) {
            this.value = value;
            count = new CountDownLatch(0);
        }

        void put(V value) {
            this.value = value;
            count.countDown();
        }

        V get(int timeout, TimeUnit unit) throws InterruptedException {
            count.await(timeout, unit);
            return value;
        }
        
        V getImmediate() {
            return value;
        }

    }
}
