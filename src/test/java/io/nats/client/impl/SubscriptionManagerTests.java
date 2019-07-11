// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.nats.client.SubscriptionManager;

public class SubscriptionManagerTests {
    @Test
    public void testSingleMessage() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("subject", (msg) -> {
                msgFuture.complete(msg);
            });
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("subject", new byte[16]);

            Message msg = msgFuture.get(500, TimeUnit.MILLISECONDS);

            assertTrue(sm.isActive());
            assertEquals("subject", msg.getSubject());
            assertNotNull(msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);
        }
    }

    @Test
    public void testMultiSubject() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final CompletableFuture<Message> one = new CompletableFuture<>();
            final CompletableFuture<Message> two = new CompletableFuture<>();
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("one", (msg) -> { one.complete(msg); });
            sm.subscribe("two", (msg) -> { two.complete(msg); });

            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("one", new byte[16]);
            nc.publish("two", new byte[16]);

            Message msg = one.get(500, TimeUnit.MILLISECONDS);
            assertEquals("one", msg.getSubject());
            msg = two.get(500, TimeUnit.MILLISECONDS);
            assertEquals("two", msg.getSubject());
        }
    }

    @Test
    public void testMultiMessage() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            SubscriptionManager sm = nc.createSubscriptionManager();

            sm.subscribe("subject", (msg) -> { q.add(msg); });
            sm.subscribe("done", (msg) -> { done.complete(Boolean.TRUE); });
            nc.flush(Duration.ofMillis(1000)); // wait for them to go through

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);
            nc.flush(Duration.ofMillis(1000)); // wait for them to go through

            done.get(500, TimeUnit.MILLISECONDS);

            assertEquals(msgCount, q.size());
        }
    }

    @Test(expected=TimeoutException.class)
    public void testClose() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> phase1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> phase2 = new CompletableFuture<>();
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            SubscriptionManager sm = nc.createSubscriptionManager();

            sm.subscribe("subject", (msg) -> { q.add(msg); });
            sm.subscribe("phase1", (msg) -> { phase1.complete(Boolean.TRUE); });
            sm.subscribe("phase2", (msg) -> { phase2.complete(Boolean.TRUE); });
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("subject", new byte[16]);
            nc.publish("phase1", null);

            nc.flush(Duration.ofMillis(1000)); // wait for them to go through
            phase1.get(200, TimeUnit.MILLISECONDS);

            assertEquals(1, q.size());

            nc.closeSubscriptionManager(sm);

            assertFalse(sm.isActive());

            // This won't arrive
            nc.publish("phase2", new byte[16]);

            nc.flush(Duration.ofMillis(1000)); // wait for them to go through
            phase2.get(200, TimeUnit.MILLISECONDS);
        }
    }


    @Test
    public void testQueueSubscribers() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                 Connection nc = Nats.connect(ts.getURI())) {
            int msgs = 100;
            AtomicInteger received = new AtomicInteger();
            AtomicInteger sub1Count = new AtomicInteger();
            AtomicInteger sub2Count = new AtomicInteger();

            final CompletableFuture<Boolean> done1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> done2 = new CompletableFuture<>();

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            SubscriptionManager sm1 = nc.createSubscriptionManager();
            sm1.subscribe("subject", "queue", (msg) -> {
                sub1Count.incrementAndGet();
                received.incrementAndGet();
            });
            sm1.subscribe("done", (msg) -> { done1.complete(Boolean.TRUE); });

            SubscriptionManager sm2 = nc.createSubscriptionManager();
            sm2.subscribe("subject", "queue", (msg) -> {
                sub2Count.incrementAndGet();
                received.incrementAndGet();
            });
            sm2.subscribe("done", (msg) -> { done2.complete(Boolean.TRUE); });
            nc.flush(Duration.ofMillis(500));

            for (int i = 0; i < msgs; i++) {
                nc.publish("subject", new byte[16]);
            }

            nc.publish("done", null);

            nc.flush(Duration.ofMillis(500));
            done1.get(500, TimeUnit.MILLISECONDS);
            done2.get(500, TimeUnit.MILLISECONDS);

            assertEquals(msgs, received.get());
            assertEquals(msgs, sub1Count.get() + sub2Count.get());

            // They won't be equal but print to make sure they are close (human testing)
            System.out.println("### Sub 1 " + sub1Count.get());
            System.out.println("### Sub 2 " + sub2Count.get());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCantUnsubSubFromSubscriptionManager()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
                try (NatsTestServer ts = new NatsTestServer(false);
                            Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("subject", (msg) -> { msgFuture.complete(msg); });

            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("subject", new byte[16]);

            Message msg = msgFuture.get(500, TimeUnit.MILLISECONDS);

            msg.getSubscription().unsubscribe(); // Should throw
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCantAutoUnsubSubFromSubscriptionManager()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
                try (NatsTestServer ts = new NatsTestServer(false);
                            Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("subject", (msg) -> { msgFuture.complete(msg); });
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("subject", new byte[16]);

            Message msg = msgFuture.get(500, TimeUnit.MILLISECONDS);

            msg.getSubscription().unsubscribe(1); // Should throw
            assertFalse(true);
        }
    }

    @Test
    public void testPublishAndFlushFromCallback()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("subject", (msg) -> {
                try {
                    nc.flush(Duration.ofMillis(1000));
                } catch (Exception ex) {
                    System.out.println("!!! Exception in callback");
                    ex.printStackTrace();
                }
                msgFuture.complete(msg);
            });

            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("subject", new byte[16]); // publish one to kick it off

            Message msg = msgFuture.get(500, TimeUnit.MILLISECONDS);
            assertNotNull(msg);

            assertEquals(2, ((NatsStatistics)(nc.getStatistics())).getFlushCounter());
        }
    }

    @Test
    public void testUnsub() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> phase1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> phase2 = new CompletableFuture<>();
            int msgCount = 10;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            SubscriptionManager sm = nc.createSubscriptionManager();

            Subscription s1 = sm.subscribe("subject", (msg) -> { q.add(msg); });
            sm.subscribe("phase1", (msg) -> { phase1.complete(Boolean.TRUE); });
            sm.subscribe("phase2", (msg) -> { phase2.complete(Boolean.TRUE); });

            nc.flush(Duration.ofMillis(1000));// Get them all to the server

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("phase1", new byte[16]);
            nc.flush(Duration.ofMillis(1000)); // wait for them to go through

            phase1.get(5000, TimeUnit.MILLISECONDS);

            sm.unsubscribe(s1);
            nc.flush(Duration.ofMillis(1000));// Get them all to the server

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("phase2", new byte[16]);
            nc.flush(Duration.ofMillis(1000)); // wait for them to go through

            phase2.get(1000, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(msgCount, q.size());
        }
    }

    @Test
    public void testAutoUnsub() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> phase1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> phase2 = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            SubscriptionManager sm = nc.createSubscriptionManager();

            Subscription s1 = sm.subscribe("subject", (msg) -> { q.add(msg); });
            sm.subscribe("phase1", (msg) -> { phase1.complete(Boolean.TRUE); });
            sm.subscribe("phase2", (msg) -> { phase2.complete(Boolean.TRUE); });

            nc.flush(Duration.ofMillis(500));// Get them all to the server

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("phase1", new byte[16]);

            nc.flush(Duration.ofMillis(1000)); // wait for them to go through
            phase1.get(1000, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(msgCount, q.size());

            sm.unsubscribe(s1, msgCount + 1);

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("phase2", new byte[16]);

            nc.flush(Duration.ofMillis(1000)); // Wait for it all to get processed
            phase2.get(1000, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(msgCount + 1, q.size());
        }
    }

    @Test
    public void testUnsubFromCallback() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final AtomicReference<SubscriptionManager> subscriptionManager = new AtomicReference<>();
            final AtomicReference<Subscription> subscription = new AtomicReference<>();
            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            SubscriptionManager sm = nc.createSubscriptionManager();

            sm.subscribe("done", (msg) -> { done.complete(Boolean.TRUE); });
            Subscription s1 = sm.subscribe("subject", (msg) -> {
                q.add(msg);
                subscriptionManager.get().unsubscribe(subscription.get());
            });

            subscription.set(s1);
            subscriptionManager.set(sm);

            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("subject", new byte[16]);
            nc.publish("subject", new byte[16]);
            nc.publish("done", new byte[16]); // when we get this we know the others are dispatched
            nc.flush(Duration.ofMillis(1000)); // Wait for the publish, or we will get multiples for sure
            done.get(200, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(1, q.size());
        }
    }

    @Test
    public void testAutoUnsubFromCallback()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final AtomicReference<SubscriptionManager> subscriptionManager = new AtomicReference<>();
            final AtomicReference<Subscription> subscription = new AtomicReference<>();
            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            final SubscriptionManager sm = nc.createSubscriptionManager();

            sm.subscribe("done", (msg) -> { done.complete(Boolean.TRUE); });
            Subscription s1 = sm.subscribe("subject", (msg) -> {
                q.add(msg);
                subscriptionManager.get().unsubscribe(subscription.get(), 2); // get 1 more, for a total of 2
            });

            subscription.set(s1);
            subscriptionManager.set(sm);

            nc.flush(Duration.ofMillis(1000));// Get them all to the server

            nc.publish("subject", new byte[16]);
            nc.publish("subject", new byte[16]);
            nc.publish("subject", new byte[16]);
            nc.publish("done", new byte[16]); // when we get this we know the others are dispatched
            nc.flush(Duration.ofMillis(1000)); // Wait for the publish

            done.get(200, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(2, q.size());
        }
    }

    @Test
    public void testCloseFromCallback() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("done", (msg) -> {
                try {
                    nc.close();
                    done.complete(Boolean.TRUE);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            nc.flush(Duration.ofMillis(5000));// Get them all to the server

            nc.publish("done", new byte[16]); // when we get this we know the others are dispatched
            nc.flush(Duration.ofMillis(5000)); // Wait for the publish

            done.get(5000, TimeUnit.MILLISECONDS); // make sure we got them
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testDispatchHandlesExceptionInHandler() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("subject", (msg) -> {
                q.add(msg);
                throw new NumberFormatException();
            });
            sm.subscribe("done", (msg) -> { done.complete(Boolean.TRUE); });
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);

            nc.flush(Duration.ofMillis(1000)); // wait for them to go through
            done.get(200, TimeUnit.MILLISECONDS);

            assertEquals(msgCount, q.size());
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullSubject() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe(null, (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullHandler() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("test", null);
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptySubject() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();

            sm.subscribe("", (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager d = nc.createSubscriptionManager();
            d.subscribe("subject", null, (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullHandlerWithQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager d = nc.createSubscriptionManager();
            d.subscribe("subject", "queue", null);
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptyQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("subject", "", (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullSubjectWithQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe(null, "quque", (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptySubjectWithQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("", "quque", (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void throwsOnCreateIfClosed() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.close();
            nc.createSubscriptionManager();
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void throwsOnSubscribeIfClosed() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            nc.close();
            sm.subscribe("subject", (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testThrowOnSubscribeWhenClosed() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            nc.closeSubscriptionManager(sm);
            sm.subscribe("foo", (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testThrowOnUnsubscribeWhenClosed() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            Subscription s1 = sm.subscribe("foo", (msg) -> {});
            nc.closeSubscriptionManager(sm);
            sm.unsubscribe(s1);
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnDoubleClose() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            nc.closeSubscriptionManager(sm);
            nc.closeSubscriptionManager(sm);
            assertFalse(true);
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testThrowOnConnClosed() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            nc.close();
            nc.closeSubscriptionManager(sm);
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullSubjectInUnsub() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.unsubscribe(null);
            assertFalse(true);
        }
    }

    @Test
    public void testDoubleSubscribe() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            SubscriptionManager sm = nc.createSubscriptionManager();
            sm.subscribe("subject", (msg) -> { q.add(msg); });
            sm.subscribe("subject", (msg) -> { q.add(msg); });
            sm.subscribe("done", (msg) -> { done.complete(Boolean.TRUE); });

            nc.flush(Duration.ofMillis(500)); // wait for them to go through

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);
            nc.flush(Duration.ofMillis(500)); // wait for them to go through

            done.get(500, TimeUnit.MILLISECONDS);

            assertEquals(msgCount * 2, q.size()); // We should get 2x the messages because we subscribed 3 times.
        }
    }

    @Test
    public void testDoubleSubscribeWithUnsubscribeAfter() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            SubscriptionManager sm = nc.createSubscriptionManager();
            Subscription s1 = sm.subscribe("subject", (msg) -> { q.add(msg); });
            sm.subscribe("subject", (msg) -> { q.add(msg); });
            sm.subscribe("done", (msg) -> { done.complete(Boolean.TRUE); });

            nc.flush(Duration.ofMillis(500)); // wait for them to go through

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);
            nc.flush(Duration.ofMillis(500)); // wait for them to go through

            done.get(500, TimeUnit.MILLISECONDS);

            assertEquals(msgCount * 2, q.size()); // We should get 2x the messages because we subscribed 3 times.

            q.clear();
            sm.unsubscribe(s1);

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);
            nc.flush(Duration.ofMillis(500)); // wait for them to go through

            done.get(500, TimeUnit.MILLISECONDS);

            assertEquals(msgCount, q.size()); // We only have 1 active subscription, so we should only get msgCount.
        }
    }

}
