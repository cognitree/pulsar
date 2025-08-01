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
package org.apache.bookkeeper.mledger.offload.jcloud;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlobStoreBackedInputStreamImpl;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class BlobStoreBackedInputStreamTest extends BlobStoreTestBase {

    class RandomInputStream extends InputStream {
        final Random r;
        int bytesRemaining;

        RandomInputStream(int seed, int bytesRemaining) {
            this.r = new Random(seed);
            this.bytesRemaining = bytesRemaining;
        }

        @Override
        public int read() {
            if (bytesRemaining-- > 0) {
                return r.nextInt() & 0xFF;
            } else {
                return -1;
            }
        }
    }

    private void assertStreamsMatch(BackedInputStream a, InputStream b, long initialPosition) throws Exception {
        assertEquals(initialPosition, a.getCurrentPosition());
        int ret = 0;
        long expectedPosition = initialPosition;
        while (ret >= 0) {
            ret = a.read();
            assertEquals(ret, b.read());
            if (ret != -1) {
                // reached end of the stream, so read() did not advance the position
                expectedPosition++;
            }
            assertEquals(a.getCurrentPosition(), expectedPosition);
        }
        assertEquals(-1, a.read());
        assertEquals(-1, b.read());
    }

    private void assertStreamsMatchByBytes(BackedInputStream a, InputStream b) throws Exception {
        byte[] bytesA = new byte[100];
        byte[] bytesB = new byte[100];

        int retA = 0;
        long expectedPosition = 0;
        while (retA >= 0) {
            retA = a.read(bytesA, 0, 100);
            int retB = b.read(bytesB, 0, 100);
            assertEquals(retA, retB);
            assertEquals(bytesA, bytesB);
            if (retA != -1) {
                // reached end of the stream, so read() did not advance the position
                expectedPosition += retA;
            }
            assertEquals(a.getCurrentPosition(), expectedPosition);
        }
    }

    @Test
    public void testReadingFullObject() throws Exception {
        String objectKey = "testReadingFull";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);
        RandomInputStream toCompare = new RandomInputStream(0, objectSize);

        Payload payload = Payloads.newInputStreamPayload(toWrite);
        payload.getContentMetadata().setContentLength((long) objectSize);
        Blob blob = blobStore.blobBuilder(objectKey)
            .payload(payload)
            .contentLength((long) objectSize)
            .build();
        String ret = blobStore.putBlob(BUCKET, blob);
        log.debug("put blob: {} in Bucket: {}, in blobStore, result: {}", objectKey, BUCKET, ret);

        @Cleanup
        BackedInputStream toTest = new BlobStoreBackedInputStreamImpl(blobStore, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);
        assertStreamsMatch(toTest, toCompare, 0);
    }

    @Test
    public void testReadingFullObjectByBytes() throws Exception {
        String objectKey = "testReadingFull2";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);
        RandomInputStream toCompare = new RandomInputStream(0, objectSize);

        Payload payload = Payloads.newInputStreamPayload(toWrite);
        payload.getContentMetadata().setContentLength((long) objectSize);
        Blob blob = blobStore.blobBuilder(objectKey)
            .payload(payload)
            .contentLength((long) objectSize)
            .build();
        String ret = blobStore.putBlob(BUCKET, blob);
        log.debug("put blob: {} in Bucket: {}, in blobStore, result: {}", objectKey, BUCKET, ret);

        @Cleanup
        BackedInputStream toTest = new BlobStoreBackedInputStreamImpl(blobStore, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);
        assertStreamsMatchByBytes(toTest, toCompare);
    }

    @Test(expectedExceptions = KeyNotFoundException.class)
    public void testNotFoundOnRead() throws Exception {
        @Cleanup
        BackedInputStream toTest = new BlobStoreBackedInputStreamImpl(blobStore, BUCKET, "doesn't exist",
                                                                 (key, md) -> {},
                                                                 1234, 1000);
        toTest.read();
    }


    @Test
    public void testSeek() throws Exception {
        String objectKey = "testSeek";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);

        Map<Integer, InputStream> seeks = new HashMap<>();
        Random r = new Random(12345);
        for (int i = 0; i < 20; i++) {
            int seek = r.nextInt(objectSize + 1);
            RandomInputStream stream = new RandomInputStream(0, objectSize);
            stream.skip(seek);
            seeks.put(seek, stream);
        }

        Payload payload = Payloads.newInputStreamPayload(toWrite);
        payload.getContentMetadata().setContentLength((long) objectSize);
        Blob blob = blobStore.blobBuilder(objectKey)
            .payload(payload)
            .contentLength((long) objectSize)
            .build();
        String ret = blobStore.putBlob(BUCKET, blob);
        log.debug("put blob: {} in Bucket: {}, in blobStore, result: {}", objectKey, BUCKET, ret);

        @Cleanup
        BackedInputStream toTest = new BlobStoreBackedInputStreamImpl(blobStore, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);
        for (Map.Entry<Integer, InputStream> e : seeks.entrySet()) {
            toTest.seek(e.getKey());
            assertStreamsMatch(toTest, e.getValue(), e.getKey().longValue());
        }
    }

    @Test
    public void testSeekWithinCurrent() throws Exception {
        String objectKey = "testSeekWithinCurrent";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);

        Payload payload = Payloads.newInputStreamPayload(toWrite);
        payload.getContentMetadata().setContentLength((long) objectSize);
        Blob blob = blobStore.blobBuilder(objectKey)
            .payload(payload)
            .contentLength((long) objectSize)
            .build();
        String ret = blobStore.putBlob(BUCKET, blob);
        log.debug("put blob: {} in Bucket: {}, in blobStore, result: {}", objectKey, BUCKET, ret);

        //BlobStore spiedBlobStore = spy(blobStore);
        BlobStore spiedBlobStore = mock(BlobStore.class, delegatesTo(blobStore));

        @Cleanup
        BackedInputStream toTest = new BlobStoreBackedInputStreamImpl(spiedBlobStore, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);

        // seek forward
        RandomInputStream firstSeek = new RandomInputStream(0, objectSize);
        toTest.seek(100);
        firstSeek.skip(100);
        for (int i = 0; i < 100; i++) {
            assertEquals(firstSeek.read(), toTest.read());
        }

        // seek forward a bit more, but in same block
        RandomInputStream secondSeek = new RandomInputStream(0, objectSize);
        toTest.seek(600);
        secondSeek.skip(600);
        for (int i = 0; i < 100; i++) {
            assertEquals(secondSeek.read(), toTest.read());
        }

        // seek back
        RandomInputStream thirdSeek = new RandomInputStream(0, objectSize);
        toTest.seek(200);
        thirdSeek.skip(200);
        for (int i = 0; i < 100; i++) {
            assertEquals(thirdSeek.read(), toTest.read());
        }

        verify(spiedBlobStore, times(1))
            .getBlob(Mockito.eq(BUCKET), Mockito.eq(objectKey), ArgumentMatchers.any());
    }

    @Test
    public void testSeekForward() throws Exception {
        String objectKey = "testSeekForward";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);

        Payload payload = Payloads.newInputStreamPayload(toWrite);
        payload.getContentMetadata().setContentLength((long) objectSize);
        Blob blob = blobStore.blobBuilder(objectKey)
            .payload(payload)
            .contentLength((long) objectSize)
            .build();
        String ret = blobStore.putBlob(BUCKET, blob);
        log.debug("put blob: {} in Bucket: {}, in blobStore, result: {}", objectKey, BUCKET, ret);

        @Cleanup
        BackedInputStream toTest = new BlobStoreBackedInputStreamImpl(blobStore, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);

        // seek forward to middle
        long middle = objectSize / 2;
        toTest.seekForward(middle);

        try {
            long before = middle - objectSize / 4;
            toTest.seekForward(before);
            Assert.fail("Shound't be able to seek backwards");
        } catch (IOException ioe) {
            // correct
        }

        long after = middle + objectSize / 4;
        RandomInputStream toCompare = new RandomInputStream(0, objectSize);
        toCompare.skip(after);

        toTest.seekForward(after);
        assertStreamsMatch(toTest, toCompare, after);
    }

    @Test
    public void testAvailable() throws IOException {
        String objectKey = "testAvailable";
        int objectSize = 2048;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);
        Payload payload = Payloads.newInputStreamPayload(toWrite);
        payload.getContentMetadata().setContentLength((long) objectSize);
        Blob blob = blobStore.blobBuilder(objectKey)
            .payload(payload)
            .contentLength(objectSize)
            .build();
        String ret = blobStore.putBlob(BUCKET, blob);
        @Cleanup
        BackedInputStream bis = new BlobStoreBackedInputStreamImpl(
            blobStore, BUCKET, objectKey, (k, md) -> {}, objectSize, 512);
        assertEquals(bis.available(), objectSize);
        bis.seek(500);
        assertEquals(bis.available(), objectSize - 500);
        bis.seek(1024);
        assertEquals(bis.available(), 1024);
        bis.seek(2048);
        assertEquals(bis.available(), 0);
    }
}
