/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.genai;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.genai.types.GenerateContentResponse;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.Okio;
import okio.Source;
import okio.Timeout;
import org.junit.jupiter.api.Test;

public final class ResponseStreamCloseTest {

  @Test
  public void testCloseUnblocksReadInProgress() throws Exception {
    BlockingSource source = new BlockingSource();
    ResponseBody body =
        ResponseBody.create(MediaType.parse("text/event-stream"), -1, Okio.buffer(source));
    // Use the SDK's real response wrapper and OkHttp's real close path; no network is needed.
    HttpApiResponse response =
        new HttpApiResponse(
            new Response.Builder()
                .request(new Request.Builder().url("https://example.com/stream").build())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(body)
                .build());
    ResponseStream<GenerateContentResponse> stream =
        new ResponseStream<>(
            GenerateContentResponse.class, response, new DummyConverter(), "convert");
    ExecutorService executor =
        Executors.newFixedThreadPool(
            2,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setDaemon(true);
              return thread;
            });

    try {
      Future<Boolean> reading = executor.submit(() -> stream.iterator().hasNext());
      // Once the source is reading, BufferedReader.readLine() already holds the reader's lock.
      assertTrue(source.readStarted.await(5, TimeUnit.SECONDS), "Stream read did not start");

      Future<?> closing = executor.submit(() -> stream.close());

      // Closing the response body must release the read before reader.close() waits for its lock.
      assertTrue(
          source.closed.await(5, TimeUnit.SECONDS),
          "Response body was not closed while a read held the reader lock;"
              + " close the HTTP response before closing the reader");
      closing.get(5, TimeUnit.SECONDS);
      assertFalse(reading.get(5, TimeUnit.SECONDS), "Closing the body should end the stream");
    } finally {
      // Also release the read on failure, so the broken implementation cannot hang the test suite.
      source.releaseRead.countDown();
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Stream workers did not finish");
      stream.close();
    }
  }

  @Test
  public void testCloseClosesReaderWhenResponseCloseThrows() {
    BlockingSource source = new BlockingSource();
    ResponseBody body =
        ResponseBody.create(MediaType.parse("text/event-stream"), -1, Okio.buffer(source));
    IllegalStateException closeFailure = new IllegalStateException("Response close failed");
    ApiResponse response =
        new ApiResponse() {
          @Override
          public ResponseBody getBody() {
            return body;
          }

          @Override
          public Headers getHeaders() {
            return Headers.of();
          }

          @Override
          public void close() {
            throw closeFailure;
          }
        };
    ResponseStream<GenerateContentResponse> stream =
        new ResponseStream<>(
            GenerateContentResponse.class, response, new DummyConverter(), "convert");

    try {
      assertSame(closeFailure, assertThrows(IllegalStateException.class, stream::close));
      // The response threw before closing its body, so only reader cleanup can close this source.
      assertTrue(source.closed.getCount() == 0, "Reader cleanup did not close the response body");
    } finally {
      body.close();
    }
  }

  public static final class DummyConverter {
    public JsonNode convert(JsonNode fromObject, ObjectNode parentObject) {
      return fromObject;
    }
  }

  private static final class BlockingSource implements Source {
    private final CountDownLatch readStarted = new CountDownLatch(1);
    private final CountDownLatch releaseRead = new CountDownLatch(1);
    private final CountDownLatch closed = new CountDownLatch(1);

    @Override
    public long read(Buffer sink, long byteCount) throws IOException {
      readStarted.countDown();
      try {
        releaseRead.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for the response body to close", e);
      }
      return -1;
    }

    @Override
    public Timeout timeout() {
      return Timeout.NONE;
    }

    @Override
    public void close() {
      closed.countDown();
      releaseRead.countDown();
    }
  }
}
