/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.UnmodifiableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public interface CloseableIterator<T>
        extends Iterator<T>, Closeable
{
    static <E> CloseableIterator<E> empty()
    {
        return withClose(Collections.emptyIterator());
    }

    static <E> CloseableIterator<E> withClose(Iterator<E> iterator)
    {
        if (iterator instanceof CloseableIterator) {
            return (CloseableIterator<E>) iterator;
        }

        return new CloseableIterator<E>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public E next()
            {
                return iterator.next();
            }

            @Override
            public void close()
                    throws IOException
            {
                if (iterator instanceof Closeable) {
                    ((Closeable) iterator).close();
                }
            }
        };
    }

    /**
     * Returns a view containing the result of applying {@code mapper} to each element of {@code
     * fromIterator}.
     */
    static <I, O> CloseableIterator<O> map(CloseableIterator<I> fromIterator, Function<I, O> mapper)
    {
        requireNonNull(fromIterator, "fromIterator is null");
        requireNonNull(mapper, "mapper is null");

        return new CloseableIterator<O>()
        {
            @Override
            public boolean hasNext()
            {
                return fromIterator.hasNext();
            }

            @Override
            public O next()
            {
                return mapper.apply(fromIterator.next());
            }

            @Override
            public void close()
                    throws IOException
            {
                fromIterator.close();
            }
        };
    }

    static <I, O> CloseableIterator<O> flatMap(CloseableIterator<I> fromIterator, Function<I, Iterator<O>> mapper)
    {
        return new FlatMapCloseableIterator<>(fromIterator, mapper);
    }

    class FlatMapCloseableIterator<I, O>
            extends AbstractIterator<O>
            implements CloseableIterator<O>
    {
        private final CloseableIterator<I> fromIterator;
        private final Function<I, Iterator<O>> mapper;

        private Iterator<O> mappedIterator;

        public FlatMapCloseableIterator(CloseableIterator<I> fromIterator, Function<I, Iterator<O>> mapper)
        {
            this.fromIterator = requireNonNull(fromIterator, "fromIterator is null");
            this.mapper = requireNonNull(mapper, "mapper is null");
        }

        @Override
        protected O computeNext()
        {
            while (mappedIterator == null || !mappedIterator.hasNext()) {
                if (!fromIterator.hasNext()) {
                    return endOfData();
                }
                mappedIterator = mapper.apply(fromIterator.next());
            }
            return mappedIterator.next();
        }

        @Override
        public void close()
                throws IOException
        {
            fromIterator.close();
        }
    }

    static <T> CloseableIterator<T> filter(CloseableIterator<T> unfiltered, Predicate<? super T> retainIfTrue)
    {
        return new FilteredCloseableIterator<>(unfiltered, retainIfTrue);
    }

    class FilteredCloseableIterator<T>
            extends AbstractIterator<T>
            implements CloseableIterator<T>
    {
        private final CloseableIterator<T> unfiltered;
        private final Predicate<? super T> predicate;

        public FilteredCloseableIterator(CloseableIterator<T> unfiltered, Predicate<? super T> predicate)
        {
            this.unfiltered = requireNonNull(unfiltered, "unfiltered is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
        }

        @Override
        protected T computeNext()
        {
            while (unfiltered.hasNext()) {
                T element = unfiltered.next();
                if (predicate.apply(element)) {
                    return element;
                }
            }
            return endOfData();
        }

        @Override
        public void close()
                throws IOException
        {
            unfiltered.close();
        }
    }

    /**
     * Combines two closeable iterators into a single iterator. The returned iterator iterates across the
     * elements in {@code first}, followed by the elements in {@code second}. The source iterators are not
     * polled until necessary.
     *
     * <p>The returned iterator supports {@code remove()} when the corresponding input iterator
     * supports it.
     */
    static <T> CloseableIterator<T> concat(CloseableIterator<T> first, CloseableIterator<T> second)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        return concat(consumingForArray(first, second));
    }

    /**
     * Combines multiple iterators into a single iterator. The returned iterator iterates across the
     * elements of each iterator in {@code inputs}. The input iterators are not polled until
     * necessary.
     *
     * <p>The returned iterator supports {@code remove()} when the corresponding input iterator
     * supports it. The methods of the returned iterator may throw {@code NullPointerException} if any
     * of the input iterators is null.
     */
    static <T> CloseableIterator<T> concat(Iterator<CloseableIterator<T>> inputs)
    {
        return new ConcatenatedCloseableIterator<>(inputs);
    }

    /**
     * Sligh adaptation of Guava's `com.google.common.collect.Iterators.ConcatenatedIterator`
     * which provides `close()` logic.
     */
    class ConcatenatedCloseableIterator<T>
            implements CloseableIterator<T>
    {
        /* The iterator currently returning elements. */
        private CloseableIterator<? extends T> iterator;

        /*
         * We track the "meta iterators," the iterators-of-iterators, below.  Usually, topMetaIterator
         * is the only one in use, but if we encounter nested concatenations, we start a deque of
         * meta-iterators rather than letting the nesting get arbitrarily deep.  This keeps each
         * operation O(1).
         */
        private Iterator<CloseableIterator<T>> topMetaIterator;

        // Only becomes nonnull if we encounter nested concatenations.
        private Deque<Iterator<CloseableIterator<T>>> metaIterators;

        ConcatenatedCloseableIterator(Iterator<CloseableIterator<T>> metaIterator)
        {
            iterator = empty();
            topMetaIterator = requireNonNull(metaIterator, "metaIterator is null");
        }

        // Returns a nonempty meta-iterator or, if all meta-iterators are empty, null.
        private Iterator<CloseableIterator<T>> getTopMetaIterator()
        {
            while (topMetaIterator == null || !topMetaIterator.hasNext()) {
                if (metaIterators != null && !metaIterators.isEmpty()) {
                    topMetaIterator = metaIterators.removeFirst();
                }
                else {
                    return null;
                }
            }
            return topMetaIterator;
        }

        @Override
        public boolean hasNext()
        {
            while (!requireNonNull(iterator).hasNext()) {
                // this weird checkNotNull positioning appears required by our tests, which expect
                // both hasNext and next to throw NPE if an input iterator is null.
                topMetaIterator = getTopMetaIterator();
                if (topMetaIterator == null) {
                    return false;
                }

                iterator = topMetaIterator.next();

                if (iterator instanceof ConcatenatedCloseableIterator) {
                    // Instead of taking linear time in the number of nested concatenations, unpack
                    // them into the queue
                    @SuppressWarnings("unchecked")
                    ConcatenatedCloseableIterator<T> topConcat = (ConcatenatedCloseableIterator<T>) iterator;
                    iterator = topConcat.iterator;

                    // topConcat.topMetaIterator, then topConcat.metaIterators, then this.topMetaIterator,
                    // then this.metaIterators
                    if (this.metaIterators == null) {
                        this.metaIterators = new ArrayDeque<>();
                    }
                    this.metaIterators.addFirst(this.topMetaIterator);
                    if (topConcat.metaIterators != null) {
                        while (!topConcat.metaIterators.isEmpty()) {
                            this.metaIterators.addFirst(topConcat.metaIterators.removeLast());
                        }
                    }
                    this.topMetaIterator = topConcat.topMetaIterator;
                }
            }
            return true;
        }

        @Override
        public T next()
        {
            if (hasNext()) {
                return iterator.next();
            }
            else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void close()
                throws IOException
        {
            iterator.close();

            topMetaIterator = getTopMetaIterator();
            if (topMetaIterator == null) {
                return;
            }
            while (topMetaIterator.hasNext()) {
                topMetaIterator.next().close();
            }
        }
    }

    /**
     * Returns an Iterator that walks the specified array, nulling out elements behind it. This can
     * avoid memory leaks when an element is no longer necessary.
     *
     * <p>This method accepts an array with element type {@code @Nullable T}, but callers must pass an
     * array whose contents are initially non-null. The {@code @Nullable} annotation indicates that
     * this method will write nulls into the array during iteration.
     *
     */
    private static <I extends CloseableIterator<?>> Iterator<I> consumingForArray(I... elements)
    {
        return new UnmodifiableIterator<>()
        {
            int index;

            @Override
            public boolean hasNext()
            {
                return index < elements.length;
            }

            @Override
            public I next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                /*
                 * requireNonNull is safe because our callers always pass non-null arguments. Each element
                 * of the array becomes null only when we iterate past it and then clear it.
                 */
                I result = requireNonNull(elements[index]);
                elements[index] = null;
                index++;
                return result;
            }
        };
    }
}
