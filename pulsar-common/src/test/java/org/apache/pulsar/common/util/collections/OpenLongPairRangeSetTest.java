/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.util.collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.LongPair;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.LongPairConsumer;
import org.apache.pulsar.common.util.collections.LongPairRangeSet.RangeBoundConsumer;
import org.testng.annotations.Test;

public class OpenLongPairRangeSetTest {

    static final LongPairConsumer<LongPair> CONSUMER = LongPair::new;
    static final RangeBoundConsumer<LongPair> REVERSE_CONSUMER = pair -> pair;

    @Test
    public void testIsEmpty() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        assertTrue(set.isEmpty());
        // lowerValueOpen and upperValue are both -1 so that an empty set will be added
        set.addOpenClosed(0, -1, 0, -1);
        assertTrue(set.isEmpty());
        set.addOpenClosed(1, 1, 1, 5);
        assertFalse(set.isEmpty());
    }

    @Test
    public void testAddForSameKey() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        // add 0 to 5
        set.add(Range.closed(new LongPair(0, 0), new LongPair(0, 5)));
        // add 8,9,10
        set.add(Range.closed(new LongPair(0, 8), new LongPair(0, 8)));
        set.add(Range.closed(new LongPair(0, 9), new LongPair(0, 9)));
        set.add(Range.closed(new LongPair(0, 10), new LongPair(0, 10)));
        // add 98 to 99 and 102,105
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 102), new LongPair(0, 106)));

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(0, -1), new LongPair(0, 5))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(0, 7), new LongPair(0, 10))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(0, 97), new LongPair(0, 99))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(0, 101), new LongPair(0, 106))));
    }

    @Test
    public void testAddForDifferentKey() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        // [98,100],[(1,5),(1,5)],[(1,10,1,15)],[(1,20),(1,20)],[(2,0),(2,10)]
        set.addOpenClosed(0, 98, 0, 99);
        set.addOpenClosed(0, 100, 1, 5);
        set.addOpenClosed(1, 10, 1, 15);
        set.addOpenClosed(1, 20, 2, 10);

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(0, 98), new LongPair(0, 99))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(1, -1), new LongPair(1, 5))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(1, 10), new LongPair(1, 15))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(2, -1), new LongPair(2, 10))));
    }

    @Test
    public void testAddCompareCompareWithGuava() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        com.google.common.collect.RangeSet<LongPair> gSet = TreeRangeSet.create();

        // add 10K values for key 0
        int totalInsert = 10_000;
        // add single values
        for (int i = 0; i < totalInsert; i++) {
            if (i % 3 == 0 || i % 6 == 0 || i % 8 == 0) {
                LongPair lower = new LongPair(0, i - 1);
                LongPair upper = new LongPair(0, i);
                // set.add(Range.openClosed(lower, upper));
                set.addOpenClosed(lower.getKey(), lower.getValue(), upper.getKey(), upper.getValue());
                gSet.add(Range.openClosed(lower, upper));
            }
        }
        // add batches
        for (int i = totalInsert; i < (totalInsert * 2); i++) {
            if (i % 5 == 0) {
                LongPair lower = new LongPair(0, i - 3 - 1);
                LongPair upper = new LongPair(0, i + 3);
                // set.add(Range.openClosed(lower, upper));
                set.addOpenClosed(lower.getKey(), lower.getValue(), upper.getKey(), upper.getValue());
                gSet.add(Range.openClosed(lower, upper));
            }
        }
        List<Range<LongPair>> ranges = set.asRanges();
        Set<Range<LongPair>> gRanges = gSet.asRanges();

        List<Range<LongPair>> gRangeConnected = getConnectedRange(gRanges);
        assertEquals(gRangeConnected.size(), ranges.size());
        int i = 0;
        for (Range<LongPair> range : gRangeConnected) {
            assertEquals(range, ranges.get(i));
            i++;
        }
    }

    @Test
    public void testNPE() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        assertNull(set.span());
    }

    @Test
    public void testDeleteCompareWithGuava() {

        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        com.google.common.collect.RangeSet<LongPair> gSet = TreeRangeSet.create();

        // add 10K values for key 0
        int totalInsert = 10_000;
        // add single values
        List<Range<LongPair>> removedRanges = new ArrayList<>();
        for (int i = 0; i < totalInsert; i++) {
            if (i % 3 == 0 || i % 7 == 0 || i % 11 == 0) {
                continue;
            }
            LongPair lower = new LongPair(0, i - 1);
            LongPair upper = new LongPair(0, i);
            Range<LongPair> range = Range.openClosed(lower, upper);
            // set.add(range);
            set.addOpenClosed(lower.getKey(), lower.getValue(), upper.getKey(), upper.getValue());
            gSet.add(range);
            if (i % 4 == 0) {
                removedRanges.add(range);
            }
        }
        // add batches
        for (int i = totalInsert; i < (totalInsert * 2); i++) {
            LongPair lower = new LongPair(0, i - 3 - 1);
            LongPair upper = new LongPair(0, i + 3);
            Range<LongPair> range = Range.openClosed(lower, upper);
            if (i % 5 != 0) {
                // set.add(range);
                set.addOpenClosed(lower.getKey(), lower.getValue(), upper.getKey(), upper.getValue());
                gSet.add(range);
            }
            if (i % 4 == 0) {
                removedRanges.add(range);
            }
        }
        // remove records
        for (Range<LongPair> range : removedRanges) {
            set.remove(range);
            gSet.remove(range);
        }

        List<Range<LongPair>> ranges = set.asRanges();
        Set<Range<LongPair>> gRanges = gSet.asRanges();
        List<Range<LongPair>> gRangeConnected = getConnectedRange(gRanges);
        assertEquals(gRangeConnected.size(), ranges.size());
        int i = 0;
        for (Range<LongPair> range : gRangeConnected) {
            assertEquals(range, ranges.get(i));
            i++;
        }
    }

    @Test
    public void testRemoveRangeInSameKey() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        set.addOpenClosed(0, 1, 0, 50);
        set.addOpenClosed(0, 97, 0, 99);
        set.addOpenClosed(0, 99, 1, 5);
        set.addOpenClosed(1, 9, 1, 15);
        set.addOpenClosed(1, 19, 2, 10);
        set.addOpenClosed(2, 24, 2, 28);
        set.addOpenClosed(3, 11, 3, 20);
        set.addOpenClosed(4, 11, 4, 20);
        // range is [(0:1..0:50],(0:97..0:99],(1:-1..1:5],(1:9..1:15],(2:-1..2:10],(2:24..2:28],(3:11..3:20],
        // (4:11..4:20]]
        set.remove(Range.closed(new LongPair(0, 0), new LongPair(0, Integer.MAX_VALUE - 1)));
        // after remove is [(1:-1..1:5],(1:9..1:15],(2:-1..2:10],(2:24..2:28],(3:11..3:20],(4:11..4:20]]
        int count = 0;
        List<Range<LongPair>> ranges = set.asRanges();
        assertEquals(ranges.get(count++), Range.openClosed(new LongPair(1, -1), new LongPair(1, 5)));
        assertEquals(ranges.get(count++), Range.openClosed(new LongPair(1, 9), new LongPair(1, 15)));
        assertEquals(ranges.get(count++), Range.openClosed(new LongPair(2, -1), new LongPair(2, 10)));
        assertEquals(ranges.get(count++), Range.openClosed(new LongPair(2, 24), new LongPair(2, 28)));
        assertEquals(ranges.get(count++), Range.openClosed(new LongPair(3, 11), new LongPair(3, 20)));
        assertEquals(ranges.get(count), Range.openClosed(new LongPair(4, 11), new LongPair(4, 20)));
    }

    @Test
    public void testSpanWithGuava() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        com.google.common.collect.RangeSet<LongPair> gSet = TreeRangeSet.create();
        set.add(Range.openClosed(new LongPair(0, 97), new LongPair(0, 99)));
        gSet.add(Range.openClosed(new LongPair(0, 97), new LongPair(0, 99)));
        set.add(Range.openClosed(new LongPair(0, 99), new LongPair(1, 5)));
        gSet.add(Range.openClosed(new LongPair(0, 99), new LongPair(1, 5)));
        assertEquals(set.span(), gSet.span());
        assertEquals(set.span(), Range.openClosed(new LongPair(0, 97), new LongPair(1, 5)));

        set.add(Range.openClosed(new LongPair(1, 9), new LongPair(1, 15)));
        set.add(Range.openClosed(new LongPair(1, 19), new LongPair(2, 10)));
        set.add(Range.openClosed(new LongPair(2, 24), new LongPair(2, 28)));
        set.add(Range.openClosed(new LongPair(3, 11), new LongPair(3, 20)));
        set.add(Range.openClosed(new LongPair(4, 11), new LongPair(4, 20)));
        gSet.add(Range.openClosed(new LongPair(1, 9), new LongPair(1, 15)));
        gSet.add(Range.openClosed(new LongPair(1, 19), new LongPair(2, 10)));
        gSet.add(Range.openClosed(new LongPair(2, 24), new LongPair(2, 28)));
        gSet.add(Range.openClosed(new LongPair(3, 11), new LongPair(3, 20)));
        gSet.add(Range.openClosed(new LongPair(4, 11), new LongPair(4, 20)));
        assertEquals(set.span(), gSet.span());
        assertEquals(set.span(), Range.openClosed(new LongPair(0, 97), new LongPair(4, 20)));
    }

    @Test
    public void testFirstRange() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        assertNull(set.firstRange());
        Range<LongPair> range = Range.openClosed(new LongPair(0, 97), new LongPair(0, 99));
        set.add(range);
        assertEquals(set.firstRange(), range);
        assertEquals(set.size(), 1);
        range = Range.openClosed(new LongPair(0, 98), new LongPair(0, 105));
        set.add(range);
        assertEquals(set.firstRange(), Range.openClosed(new LongPair(0, 97), new LongPair(0, 105)));
        assertEquals(set.size(), 1);
        range = Range.openClosed(new LongPair(0, 5), new LongPair(0, 75));
        set.add(range);
        assertEquals(set.firstRange(), range);
        assertEquals(set.size(), 2);
    }

    @Test
    public void testLastRange() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        assertNull(set.lastRange());
        Range<LongPair> range = Range.openClosed(new LongPair(0, 97), new LongPair(0, 99));
        set.add(range);
        assertEquals(set.lastRange(), range);
        assertEquals(set.size(), 1);
        range = Range.openClosed(new LongPair(0, 98), new LongPair(0, 105));
        set.add(range);
        assertEquals(set.lastRange(), Range.openClosed(new LongPair(0, 97), new LongPair(0, 105)));
        assertEquals(set.size(), 1);
        range = Range.openClosed(new LongPair(1, 5), new LongPair(1, 75));
        set.add(range);
        assertEquals(set.lastRange(), range);
        assertEquals(set.size(), 2);
        range = Range.openClosed(new LongPair(1, 80), new LongPair(1, 120));
        set.add(range);
        assertEquals(set.lastRange(), range);
        assertEquals(set.size(), 3);
    }

    @Test
    public void testToString() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        Range<LongPair> range = Range.openClosed(new LongPair(0, 97), new LongPair(0, 99));
        set.add(range);
        assertEquals(set.toString(), "[(0:97..0:99]]");
        range = Range.openClosed(new LongPair(0, 98), new LongPair(0, 105));
        set.add(range);
        assertEquals(set.toString(), "[(0:97..0:105]]");
        range = Range.openClosed(new LongPair(0, 5), new LongPair(0, 75));
        set.add(range);
        assertEquals(set.toString(), "[(0:5..0:75],(0:97..0:105]]");
    }

    @Test
    public void testDeleteForDifferentKey() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        set.addOpenClosed(0, 97, 0, 99);
        set.addOpenClosed(0, 99, 1, 5);
        set.addOpenClosed(1, 9, 1, 15);
        set.addOpenClosed(1, 19, 2, 10);
        set.addOpenClosed(2, 24, 2, 28);
        set.addOpenClosed(3, 11, 3, 20);
        set.addOpenClosed(4, 11, 4, 20);

        // delete only (0,100)
        set.remove(Range.open(new LongPair(0, 99), new LongPair(0, 105)));

        /**
         * delete all keys from [2,27]->[4,15] : remaining [2,25..26,28], [4,16..20]
         */
        set.remove(Range.closed(new LongPair(2, 27), new LongPair(4, 15)));

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(0, 97), new LongPair(0, 99))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(1, -1), new LongPair(1, 5))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(1, 9), new LongPair(1, 15))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(2, -1), new LongPair(2, 10))));

        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(2, 24), new LongPair(2, 26))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(2, 27), new LongPair(2, 28))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(4, 15), new LongPair(4, 20))));
    }

    @Test
    public void testDeleteWithAtMost() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        set.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        set.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        set.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        set.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        set.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));

        // delete only (0,100)
        set.remove(Range.open(new LongPair(0, 99), new LongPair(0, 105)));

        /**
         * delete all keys from [2,27]->[4,15] : remaining [2,25..26,28], [4,16..20]
         */
        set.remove(Range.atMost(new LongPair(2, 27)));

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(2, 27), new LongPair(2, 28))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(3, 11), new LongPair(3, 20))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(4, 11), new LongPair(4, 20))));
    }

    @Test
    public void testDeleteWithLeastMost() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        set.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        set.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        set.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        set.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        set.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));

        // delete only (0,100)
        set.remove(Range.open(new LongPair(0, 99), new LongPair(0, 105)));

        /**
         * delete all keys from [2,27]->[4,15] : remaining [2,25..26,28], [4,16..20]
         */
        set.remove(Range.atLeast(new LongPair(2, 27)));

        List<Range<LongPair>> ranges = set.asRanges();
        int count = 0;
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(0, 97), new LongPair(0, 99))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(1, -1), new LongPair(1, 5))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(1, 9), new LongPair(1, 15))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(1, 19), new LongPair(1, 20))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(2, -1), new LongPair(2, 10))));
        assertEquals(ranges.get(count++), (Range.openClosed(new LongPair(2, 24), new LongPair(2, 26))));
    }

    @Test
    public void testRangeContaining() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        set.add(Range.closed(new LongPair(0, 98), new LongPair(0, 99)));
        set.add(Range.closed(new LongPair(0, 100), new LongPair(1, 5)));
        com.google.common.collect.RangeSet<LongPair> gSet = TreeRangeSet.create();
        gSet.add(Range.closed(new LongPair(0, 98), new LongPair(0, 100)));
        gSet.add(Range.closed(new LongPair(0, 101), new LongPair(1, 5)));
        set.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        set.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        set.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        set.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        set.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));
        gSet.add(Range.closed(new LongPair(1, 10), new LongPair(1, 15)));
        gSet.add(Range.closed(new LongPair(1, 20), new LongPair(2, 10)));
        gSet.add(Range.closed(new LongPair(2, 25), new LongPair(2, 28)));
        gSet.add(Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        gSet.add(Range.closed(new LongPair(4, 12), new LongPair(4, 20)));

        LongPair position = new LongPair(0, 99);
        assertEquals(set.rangeContaining(position.getKey(), position.getValue()),
                Range.closed(new LongPair(0, 98), new LongPair(0, 100)));
        assertEquals(set.rangeContaining(position.getKey(), position.getValue()), gSet.rangeContaining(position));

        position = new LongPair(2, 30);
        assertNull(set.rangeContaining(position.getKey(), position.getValue()));
        assertEquals(set.rangeContaining(position.getKey(), position.getValue()), gSet.rangeContaining(position));

        position = new LongPair(3, 13);
        assertEquals(set.rangeContaining(position.getKey(), position.getValue()),
                Range.closed(new LongPair(3, 12), new LongPair(3, 20)));
        assertEquals(set.rangeContaining(position.getKey(), position.getValue()), gSet.rangeContaining(position));

        position = new LongPair(3, 22);
        assertNull(set.rangeContaining(position.getKey(), position.getValue()));
        assertEquals(set.rangeContaining(position.getKey(), position.getValue()), gSet.rangeContaining(position));
    }

    /**
     * fix : #4895.
     */
    @Test
    public void testCacheFlagConflict() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        set.add(Range.openClosed(new LongPair(0, 1), new LongPair(0, 2)));
        set.add(Range.openClosed(new LongPair(0, 3), new LongPair(0, 4)));
        assertEquals(set.toString(), "[(0:1..0:2],(0:3..0:4]]");
        assertEquals(set.size(), 2);
    }

    private List<Range<LongPair>> getConnectedRange(Set<Range<LongPair>> gRanges) {
        List<Range<LongPair>> gRangeConnected = new ArrayList<>();
        Range<LongPair> lastRange = null;
        for (Range<LongPair> range : gRanges) {
            if (lastRange == null) {
                lastRange = range;
                continue;
            }
            LongPair previousUpper = lastRange.upperEndpoint();
            LongPair currentLower = range.lowerEndpoint();
            int previousUpperValue = (int) (lastRange.upperBoundType().equals(BoundType.CLOSED)
                    ? previousUpper.getValue()
                    : previousUpper.getValue() - 1);
            int currentLowerValue = (int) (range.lowerBoundType().equals(BoundType.CLOSED) ? currentLower.getValue()
                    : currentLower.getValue() + 1);
            boolean connected = (previousUpper.getKey() == currentLower.getKey())
                    ? (previousUpperValue >= currentLowerValue)
                    : false;
            if (connected) {
                lastRange = Range.closed(lastRange.lowerEndpoint(), range.upperEndpoint());
            } else {
                gRangeConnected.add(lastRange);
                lastRange = range;
            }
        }
        int lowerOpenValue = (int) (lastRange.lowerBoundType().equals(BoundType.CLOSED)
                ? (lastRange.lowerEndpoint().getValue() - 1)
                : lastRange.lowerEndpoint().getValue());
        lastRange = Range.openClosed(new LongPair(lastRange.lowerEndpoint().getKey(), lowerOpenValue),
                lastRange.upperEndpoint());
        gRangeConnected.add(lastRange);
        return gRangeConnected;
    }

    @Test
    public void testCardinality() {
        OpenLongPairRangeSet<LongPair> set = new OpenLongPairRangeSet<>(CONSUMER);
        int v = set.cardinality(0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(v, 0);
        set.addOpenClosed(1, 0, 1, 20);
        set.addOpenClosed(1, 30, 1, 90);
        set.addOpenClosed(2, 0, 3, 30);
        v = set.cardinality(1, 0, 1, 100);
        assertEquals(v, 80);
        v = set.cardinality(1, 11, 1, 100);
        assertEquals(v, 70);
        v = set.cardinality(1, 0, 1, 90);
        assertEquals(v, 80);
        v = set.cardinality(1, 0, 1, 80);
        assertEquals(v, 70);
        v = set.cardinality(1, 0, 3, 30);
        assertEquals(v, 80 + 31);
    }

    @Test
    public void testForEachResultTheSameAsForEachWithRangeBoundMapper() {
        OpenLongPairRangeSet<LongPair> set =
                new OpenLongPairRangeSet<>(CONSUMER);

        LongPairRangeSet.DefaultRangeSet<LongPair> defaultRangeSet =
                new LongPairRangeSet.DefaultRangeSet<>(CONSUMER, REVERSE_CONSUMER);

        set.addOpenClosed(1, 10, 1, 15);
        set.addOpenClosed(2, 25, 2, 28);
        set.addOpenClosed(3, 12, 3, 20);
        set.addOpenClosed(4, 12, 4, 20);

        defaultRangeSet.addOpenClosed(1, 10, 1, 15);
        defaultRangeSet.addOpenClosed(2, 25, 2, 28);
        defaultRangeSet.addOpenClosed(3, 12, 3, 20);
        defaultRangeSet.addOpenClosed(4, 12, 4, 20);


        MutableInt size = new MutableInt(0);

        List<LongPair> forEachIterResult = new ArrayList<>();
        set.forEach((range) -> {
            forEachIterResult.add(range.lowerEndpoint());
            forEachIterResult.add(range.upperEndpoint());

            size.increment();
            return true;
        });

        List<LongPair> defaultRangeSetResult = new ArrayList<>();
        List<LongPair> forEachRawRangeResult = new ArrayList<>();

        defaultRangeSet.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            defaultRangeSetResult.add(new LongPair(lowerKey, lowerValue));
            defaultRangeSetResult.add(new LongPair(upperKey, upperValue));
            return true;
        });

        set.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            forEachRawRangeResult.add(new LongPair(lowerKey, lowerValue));
            forEachRawRangeResult.add(new LongPair(upperKey, upperValue));
            return true;
        });

        assertEquals(forEachIterResult, forEachRawRangeResult);
        assertEquals(forEachIterResult, defaultRangeSetResult);

        assertEquals(size.intValue(), set.size());
        assertEquals(size.intValue(), defaultRangeSet.size());
    }
}
