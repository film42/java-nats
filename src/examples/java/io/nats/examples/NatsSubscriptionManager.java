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

import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.nats.client.SubscriptionManager;
import io.nats.client.Subscription;
import io.nats.client.MessageHandler;

class NatsSubscriptionManager extends NatsConsumer implements SubscriptionManager, Runnable {
    private MessageQueue incoming;
    private Map<String, MessageHandler> handlers;
    private Map<String, NatsSubscription> subscriptions;

    private Future<Boolean> thread;
    private final AtomicBoolean running;
    private String id;

    private Duration waitForMessage;

    NatsSubscriptionManager(NatsConnection conn) {
        super(conn);
        this.incoming = new MessageQueue(true);
        this.subscriptions = new ConcurrentHashMap<>();
        this.handlers = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
        this.waitForMessage = Duration.ofMinutes(5); // This can be long since we aren't doing anything
    }

    void start(String id) {
        this.id = id;
        this.running.set(true);
        thread = connection.getExecutor().submit(this, Boolean.TRUE);
    }

    boolean breakRunLoop() {
        return this.incoming.isDrained();
    }

    public void run() {
        try {
            while (this.running.get()) {

                NatsMessage msg = this.incoming.pop(this.waitForMessage);

                if (msg == null) {
                    if (breakRunLoop()) {
                        return;
                    } else {
                        continue;
                    }
                }

                NatsSubscription sub = msg.getNatsSubscription();

                if (sub != null && sub.isActive()) {
                    MessageHandler handler = this.handlers.get(sub.getSID());
                    if (handler != null) {
                        sub.incrementDeliveredCount();
                        this.incrementDeliveredCount();

                        try {
                            handler.onMessage(msg);
                        } catch (Exception exp) {
                            this.connection.processException(exp);
                        }
                    }
                    if (sub.reachedUnsubLimit()) {
                        this.connection.invalidate(sub);
                    }
                }

                if (breakRunLoop()) {
                    // will set the group manager to not active
                    return;
                }
            }
        } catch (InterruptedException exp) {
            if (this.running.get()){
                this.connection.processException(exp);
            } //otherwise we did it
        } finally {
            this.running.set(false);
            this.thread = null;
        }
    }

    void stop(boolean unsubscribeAll) {
        this.running.set(false);
        this.incoming.pause();

        if (this.thread != null) {
            try {
                if (!this.thread.isCancelled()) {
                    this.thread.cancel(true);
                }
            } catch (Exception exp) {
                // let it go
            }
        }

        if (unsubscribeAll) {
            this.subscriptions.forEach((subj, sub) -> {
                this.connection.unsubscribe(sub, -1);
            });
        } else {
            this.subscriptions.clear();
        }
    }

    public boolean isActive() {
        return this.running.get();
    }

    String getId() {
        return id;
    }

    MessageQueue getMessageQueue() {
        return incoming;
    }

    public List<Subscription> getSubscriptions() {
        return new ArrayList(this.subscriptions.values());
    }

    void resendSubscriptions() {
        this.subscriptions.forEach((id, sub)->{
            this.connection.sendSubscriptionMessage(sub.getSID(), sub.getSubject(), sub.getQueueName(), true);
        });
    }

    // Called by the connection when the subscription is removed
    void remove(NatsSubscription sub) {
        String sid = sub.getSubject();
        subscriptions.remove(sid);
        handlers.remove(sid);
    }

    public Subscription subscribe(String subject, MessageHandler handler) {
        return this.subscribeImpl(subject, null, handler);
    }

    public Subscription subscribe(String subject, String queueName, MessageHandler handler) {
        if (subject == null || subject.length() == 0) {
            throw new IllegalArgumentException("Subject is required in subscribe");
        }

        if (queueName == null || queueName.length() == 0) {
            throw new IllegalArgumentException("QueueName is required in subscribe");
        }

        if (handler == null) {
            throw new IllegalArgumentException("MessageHandler is required in subscribe");
        }
        return this.subscribeImpl(subject, queueName, handler);
    }

    // Assumes the subj/queuename/handler checks are done, does check for closed status
    Subscription subscribeImpl(String subject, String queueName, MessageHandler handler) {
        if (!this.running.get()) {
            throw new IllegalStateException("Dispatcher is closed");
        }

        if (this.isDraining()) {
            throw new IllegalStateException("Dispatcher is draining");
        }

        NatsSubscription sub = connection.createSubscription(subject, queueName, null, this);
        subscriptions.put(sub.getSID(), sub);
        handlers.put(sub.getSID(), handler);

        return sub;
    }

    public Subscription unsubscribe(Subscription subscription) {
        return this.unsubscribe(subscription, -1);
    }

    public Subscription unsubscribe(Subscription subscription, int after) {
        if (!this.running.get()) {
            throw new IllegalStateException("SubscriptionManager is closed");
        }

        if (isDraining()) { // No op while draining
            return subscription;
        }

        if (subscription == null) {
            throw new IllegalArgumentException("Subscription is required in unsubscribe");
        }

        // We can probably optimize this path by adding getSID() to the Subscription interface.
        if (!(subscription instanceof NatsSubscription)) {
            throw new IllegalArgumentException("This Subscription implementation is not known by SubscriptionManager");
        }
        NatsSubscription ns = ((NatsSubscription) subscription);

        // Grab the NatsSubscription to verify we weren't given a different manager's subscription.
        NatsSubscription sub = subscriptions.get(ns.getSID());

        if (sub != null) {
            this.connection.unsubscribe(sub, after); // Connection will tell us when to remove from the map
        }

        return subscription;
    }

    void sendUnsubForDrain() {
        this.subscriptions.forEach((id, sub)->{
            this.connection.sendUnsub(sub, -1);
        });
    }

    void cleanUpAfterDrain() {
        this.connection.cleanupSubscriptionManager(this);
    }

    public boolean isDrained() {
        return !isActive() && super.isDrained();
    }
}
