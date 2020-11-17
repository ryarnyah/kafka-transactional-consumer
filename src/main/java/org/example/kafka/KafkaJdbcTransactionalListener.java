package org.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class KafkaJdbcTransactionalListener<K, V> implements Runnable, Closeable {

    private static final String UPDATE_QUERY = "UPDATE CONSUMER_OFFSETS SET " +
            "GROUP_OFFSET = ? " +
            "WHERE GROUP_ID = ? AND TOPIC = ? AND PARTITION = ?";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJdbcTransactionalListener.class);
    private final KafkaHandler<K, V> kafkaHandler;
    private Consumer<K, V> kafkaConsumer;
    private final DataSource dataSource;

    private Duration pollDuration = Duration.of(100, ChronoUnit.MILLIS);

    private boolean running = true;
    private CountDownLatch countDownLatch;
    private final CountDownLatch startCountDownLatch = new CountDownLatch(1);

    private final List<String> topics;
    private final Map<String, Object> consumerConfig;

    public KafkaJdbcTransactionalListener(List<String> topics,
                                          KafkaHandler<K, V> kafkaHandler,
                                          DataSource dataSource,
                                          Map<String, Object> consumerConfig) {
        this.topics = topics;
        this.kafkaHandler = kafkaHandler;
        this.consumerConfig = consumerConfig;
        this.dataSource = dataSource;
        this.consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    }

    public void setPollDuration(Duration pollDuration) {
        this.pollDuration = pollDuration;
    }

    @Override
    public void close() {
        this.running = false;
        try {
            if (countDownLatch != null) {
                countDownLatch.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        this.kafkaConsumer.close();
    }

    public interface KafkaHandler<K, V> {
        void handleRecord(ConsumerRecord<K, V> record, Connection connection) throws Exception;
    }

    public void start() throws InterruptedException {
        countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread(this);
        thread.start();
        startCountDownLatch.await();
    }

    @Override
    public void run() {
        KafkaJdbcTransactionalConsumer<K, V> kafkaJdbcTransactionalConsumer = new KafkaJdbcTransactionalConsumer<>(consumerConfig, dataSource);
        this.kafkaConsumer = new WaitingConsumer<>(kafkaJdbcTransactionalConsumer);
        kafkaConsumer.subscribe(topics);

        while (this.running) {
            ConsumerRecords<K, V> records = this.kafkaConsumer.poll(pollDuration);
            startCountDownLatch.countDown();
            for (ConsumerRecord<K, V> record: records) {
                Connection connection = null;
                try {
                    connection = this.dataSource.getConnection();
                    connection.setAutoCommit(false);

                    this.kafkaHandler.handleRecord(record, connection);

                    try (PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_QUERY)) {
                        preparedStatement.setLong(1, record.offset() + 1);
                        preparedStatement.setString(2, kafkaJdbcTransactionalConsumer.getGroupId());
                        preparedStatement.setString(3, record.topic());
                        preparedStatement.setInt(4, record.partition());
                        preparedStatement.executeUpdate();
                    }

                    connection.commit();
                } catch (Exception e) {
                    if (connection != null) {
                        try {
                            connection.rollback();
                        } catch (SQLException e1) {
                            LOGGER.error("", e1);
                        }
                    }
                    kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                    LOGGER.error("", e);
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            LOGGER.error("", e);
                        }
                    }
                }
            }
            this.kafkaConsumer.commitAsync();
        }
        kafkaConsumer.close();
        countDownLatch.countDown();
    }
}
