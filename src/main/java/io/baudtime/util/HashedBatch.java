/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.baudtime.util;

import io.baudtime.message.Hashed;

import java.util.Collection;
import java.util.Iterator;

public class HashedBatch<E> implements Hashed, Collection<E> {

    private final int hash;
    private final Collection<? extends E> batch;

    public HashedBatch(int hash, Collection<? extends E> batch) {
        if (batch == null)
            throw new NullPointerException();
        this.hash = hash;
        this.batch = batch;
    }

    @Override
    public int hash() {
        return this.hash;
    }

    @Override
    public int size() {
        return batch.size();
    }

    @Override
    public boolean isEmpty() {
        return batch.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return batch.contains(o);
    }

    @Override
    public Object[] toArray() {
        return batch.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return batch.toArray(a);
    }

    @Override
    public String toString() {
        return batch.toString();
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            private final Iterator<? extends E> i = batch.iterator();

            public boolean hasNext() {
                return i.hasNext();
            }

            public E next() {
                return i.next();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public boolean add(E e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> coll) {
        return batch.containsAll(coll);
    }

    @Override
    public boolean addAll(Collection<? extends E> coll) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> coll) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> coll) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}