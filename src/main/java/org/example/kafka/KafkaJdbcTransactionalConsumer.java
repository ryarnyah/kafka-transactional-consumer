package org.example.kafka;

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

public class KafkaJdbcTransactionalConsumer<K, V> extends KafkaConsumer<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJdbcTransactionalConsumer.class);

    private final String groupId;
    private final DataSource dataSource;

    private String getConfigGroupId(Map<String, Object> configs,
                                    Deserializer<K> keyDeserializer,
                                    Deserializer<V> valueDeserializer) {
        ConsumerConfig config = new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer));
        return getConfigGroupId(config);
    }

    private String getConfigGroupId(ConsumerConfig config) {
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                GroupRebalanceConfig.ProtocolType.CONSUMER);
        return groupRebalanceConfig.groupId;
    }

    private String getConfigGroupId(Properties configs,
                                    Deserializer<K> keyDeserializer,
                                    Deserializer<V> valueDeserializer) {
        ConsumerConfig config = new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer));
        return getConfigGroupId(config);
    }

    public KafkaJdbcTransactionalConsumer(Map<String, Object> configs, DataSource dataSource) {
        super(configs);
        this.dataSource = dataSource;
        this.groupId = getConfigGroupId(configs, null, null);
    }

    public KafkaJdbcTransactionalConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, DataSource dataSource) {
        super(configs, keyDeserializer, valueDeserializer);
        this.dataSource = dataSource;
        this.groupId = getConfigGroupId(configs, keyDeserializer, valueDeserializer);
    }

    public KafkaJdbcTransactionalConsumer(Properties properties, DataSource dataSource) {
        super(properties);
        this.dataSource = dataSource;
        this.groupId = getConfigGroupId(properties, null, null);
    }

    public KafkaJdbcTransactionalConsumer(Properties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, DataSource dataSource) {
        super(properties, keyDeserializer, valueDeserializer);
        this.dataSource = dataSource;
        this.groupId = getConfigGroupId(properties, keyDeserializer, valueDeserializer);
    }

    public String getGroupId() {
        return groupId;
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        ConsumerRebalanceListenerMultiplexer multiplexer = new ConsumerRebalanceListenerMultiplexer();
        multiplexer.addConsumerRebalanceListener(listener);
        multiplexer.addConsumerRebalanceListener(new DatasourceConsumerRebalanceListener<>(
                this.dataSource,
                this,
                this.groupId
        ));
        super.subscribe(pattern, multiplexer);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        ConsumerRebalanceListenerMultiplexer multiplexer = new ConsumerRebalanceListenerMultiplexer();
        multiplexer.addConsumerRebalanceListener(listener);
        multiplexer.addConsumerRebalanceListener(new DatasourceConsumerRebalanceListener<>(
                this.dataSource,
                this,
                this.groupId
        ));
        super.subscribe(topics, multiplexer);
    }

    private static class ConsumerRebalanceListenerMultiplexer implements ConsumerRebalanceListener {
        private final List<ConsumerRebalanceListener> consumerRebalanceListeners = new ArrayList<>();

        public void addConsumerRebalanceListener(ConsumerRebalanceListener listener) {
            this.consumerRebalanceListeners.add(listener);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            for (ConsumerRebalanceListener listener: this.consumerRebalanceListeners) {
                listener.onPartitionsRevoked(partitions);
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (ConsumerRebalanceListener listener: this.consumerRebalanceListeners) {
                listener.onPartitionsAssigned(partitions);
            }
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            for (ConsumerRebalanceListener listener: this.consumerRebalanceListeners) {
                listener.onPartitionsLost(partitions);
            }
        }
    }

    private static class DatasourceConsumerRebalanceListener<K, V> implements ConsumerRebalanceListener {

        private final DataSource dataSource;
        private final Consumer<K, V> kafkaConsumer;
        private final String groupId;

        private static final String OFFSET_REQUEST = "SELECT GROUP_OFFSET FROM CONSUMER_OFFSETS " +
                "WHERE GROUP_ID = ? AND TOPIC = ? AND PARTITION = ?";
        private static final String INITIAL_OFFSET_REQUEST = "INSERT INTO CONSUMER_OFFSETS " +
                "(GROUP_ID, TOPIC, PARTITION, GROUP_OFFSET) VALUES (?, ?, ?, -1)";

        private DatasourceConsumerRebalanceListener(DataSource dataSource, Consumer<K, V> kafkaConsumer, String groupId) {
            this.dataSource = dataSource;
            this.kafkaConsumer = kafkaConsumer;
            this.groupId = groupId;
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // Rien à faire
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            Connection connection = null;
            try {
                connection = this.dataSource.getConnection();
                connection.setAutoCommit(false);

                for (TopicPartition topicPartition: partitions) {
                    try (PreparedStatement statement = connection.prepareStatement(OFFSET_REQUEST)) {
                        statement.setString(1, this.groupId);
                        statement.setString(2, topicPartition.topic());
                        statement.setInt(3, topicPartition.partition());
                        try (ResultSet resultSet = statement.executeQuery()) {
                            if (resultSet.next()) {
                                long offset = resultSet.getLong(1);
                                if (offset > -1) {
                                    this.kafkaConsumer.seek(topicPartition, offset);
                                }
                            } else {
                                // Insert first
                                try (PreparedStatement preparedStatement = connection.prepareStatement(INITIAL_OFFSET_REQUEST)) {
                                    preparedStatement.setString(1, this.groupId);
                                    preparedStatement.setString(2, topicPartition.topic());
                                    preparedStatement.setInt(3, topicPartition.partition());
                                    preparedStatement.executeUpdate();
                                }
                            }
                        }
                    }
                }
                connection.commit();
            } catch (Exception e) {
                if (connection != null) {
                    try {
                        connection.rollback();
                    } catch (SQLException e1) {
                        LOGGER.error("", e1);
                    }
                    throw new RuntimeException(e);
                }
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e1) {
                        LOGGER.error("", e1);
                    }
                }
            }
        }
    }
}
