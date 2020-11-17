package org.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class WaitingConsumer<K, V> implements Consumer<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(WaitingConsumer.class);

    private final Consumer<K, V> delegate;

    private LatchPartitionsAssignedListenerAdapter latchListener;

    public WaitingConsumer(Consumer<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void subscribe(Collection<String> topics) {
        latchListener = new LatchPartitionsAssignedListenerAdapter(null);
        delegate.subscribe(topics, latchListener);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        latchListener = new LatchPartitionsAssignedListenerAdapter(callback);
        delegate.subscribe(topics, latchListener);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        latchListener = new LatchPartitionsAssignedListenerAdapter(callback);
        delegate.subscribe(pattern, latchListener);
    }

    @Override
    public void subscribe(Pattern pattern) {
        latchListener = new LatchPartitionsAssignedListenerAdapter(null);
        delegate.subscribe(pattern, latchListener);
    }

    private static <K, V> void waitForPartitionAssignment(Consumer<K, V> consumer, LatchPartitionsAssignedListenerAdapter listener) {
        final long start = System.nanoTime();
        try {
            while (!listener.getLatch().await(100L, TimeUnit.MILLISECONDS)) {
                ConsumerRecords<?, ?> records = consumer.poll(Duration.ZERO);
                if (records.count() > 0) {
                    consumer.seekToBeginning(records.partitions());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long elapsed = System.nanoTime() - start;
        LOGGER.info("Waiting for partition assignment took {}ms", Duration.ofNanos(elapsed).toMillis());
    }

    private static <K, V> void waitForConsumerOffsets(Consumer<K, V> consumer, Duration timeout) {
        final long start = System.nanoTime();

        while (true) {
            boolean done = true;
            Set<TopicPartition> assignment = consumer.assignment();
            if (assignment.isEmpty()) {
                throw new IllegalStateException("No partition assigned");
            }

            for (TopicPartition topicPartition : assignment) {
                if (consumer.position(topicPartition) == -1) {
                    done = false;
                }
            }

            if (done) {
                break;
            }

            long elapsed = System.nanoTime() - start;
            if (elapsed >= timeout.toNanos()) {
                throw new IllegalStateException("Timeout while waiting for partition to be assigned");
            } else {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {
        if (latchListener != null) {
            waitForPartitionAssignment(delegate, latchListener);
            waitForConsumerOffsets(delegate, Duration.ofSeconds(1));
            latchListener = null;
        }

        return delegate.poll(timeout);
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        if (latchListener != null) {
            waitForPartitionAssignment(delegate, latchListener);
            waitForConsumerOffsets(delegate, Duration.ofSeconds(1));
            latchListener = null;
        }

        return delegate.poll(timeout);
    }

    @Override
    public Set<TopicPartition> assignment() {
        return delegate.assignment();
    }

    @Override
    public Set<String> subscription() {
        return delegate.subscription();
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        delegate.assign(partitions);
    }

    @Override
    public void unsubscribe() {
        delegate.unsubscribe();
    }

    @Override
    public void commitSync() {
        delegate.commitSync();
    }

    @Override
    public void commitSync(Duration timeout) {
        delegate.commitSync(timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        delegate.commitSync(offsets);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        delegate.commitSync(offsets, timeout);
    }

    @Override
    public void commitAsync() {
        delegate.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        delegate.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        delegate.commitAsync(offsets, callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        delegate.seek(partition, offset);
    }

    @Override
    public void seek(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        delegate.seek(topicPartition, offsetAndMetadata);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        delegate.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        delegate.seekToEnd(partitions);
    }

    @Override
    public long position(TopicPartition partition) {
        return delegate.position(partition);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return delegate.position(partition, timeout);
    }

    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return delegate.committed(partition);
    }

    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return delegate.committed(partition, timeout);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
        return delegate.committed(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
        return delegate.committed(partitions, timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return delegate.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return delegate.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return delegate.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return delegate.listTopics(timeout);
    }

    @Override
    public Set<TopicPartition> paused() {
        return delegate.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        delegate.pause(partitions);
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        delegate.resume(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return delegate.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return delegate.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return delegate.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return delegate.beginningOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return delegate.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return delegate.endOffsets(partitions, timeout);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Deprecated
    @Override
    public void close(long timeout, TimeUnit unit) {
        delegate.close(timeout, unit);
    }

    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }

    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return delegate.groupMetadata();
    }

    private static class LatchPartitionsAssignedListenerAdapter implements ConsumerRebalanceListener {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final ConsumerRebalanceListener delegate;

        public LatchPartitionsAssignedListenerAdapter(ConsumerRebalanceListener delegate) {
            this.delegate = delegate;
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (delegate != null) {
                delegate.onPartitionsAssigned(partitions);
            }
            latch.countDown();
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (delegate != null) {
                delegate.onPartitionsRevoked(partitions);
            }
        }
    }
}