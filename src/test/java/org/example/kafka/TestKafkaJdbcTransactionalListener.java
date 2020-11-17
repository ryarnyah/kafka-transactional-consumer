package org.example.kafka;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class TestKafkaJdbcTransactionalListener {

    private static final String CREATE_TABLE =
            "CREATE TABLE CONSUMER_OFFSETS (" +
                    "GROUP_ID VARCHAR(256) NOT NULL, " +
                    "TOPIC VARCHAR(256) NOT NULL, " +
                    "PARTITION INTEGER NOT NULL, " +
                    "GROUP_OFFSET BIGINT NOT NULL, " +
                    "PRIMARY KEY(GROUP_ID, TOPIC, PARTITION)" +
                    ")";
    private static final String CREATE_TABLE_TEST =
            "CREATE TABLE TEST (" +
                    "ID VARCHAR(256) NOT NULL," +
                    "TID VARCHAR(256) NOT NULL," +
                    "PRIMARY KEY(ID, TID)" +
                    ")";
    private static final String CREATE_INDEX_TEST =
            "CREATE INDEX IDX_TEST ON TEST (ID, TID)";
    private static final String CREATE_INDEX =
            "CREATE INDEX IDX_CONSUMER_OFFSET ON CONSUMER_OFFSETS (GROUP_ID, TOPIC, PARTITION)";
    private static final String TEST_TOPIC = "TEST_TOPIC";
    private static final String TEST_GROUP_ID = "GROUP_ID";

    private static DataSource DATASOURCE;
    private EmbeddedKafkaBroker KAFKA_BROKERS;
    private static final List<Thread> STOP_KAFKA_THREADS = new ArrayList<>();
    private static final Integer NUMBER_OF_MESSAGE = 200000;

    @BeforeAll
    public static void setupDatasource() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:h2:~/test");
        hikariConfig.setUsername("sa");
        hikariConfig.setPassword("");

        DATASOURCE = new HikariDataSource(hikariConfig);

        try (Connection connection = DATASOURCE.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.execute(CREATE_TABLE);
            statement.execute(CREATE_INDEX);
            statement.execute(CREATE_TABLE_TEST);
            statement.execute(CREATE_INDEX_TEST);
            statement.execute("INSERT INTO TEST VALUES('1', '1')");
        } catch (Exception e) {
            // Nothing
        }
    }

    @BeforeEach
    public void setUp() {
        KAFKA_BROKERS = new EmbeddedKafkaBroker(3);
        KAFKA_BROKERS.afterPropertiesSet();
        KAFKA_BROKERS.addTopics(new NewTopic(TEST_TOPIC, 3, (short) 3));
    }

    @AfterEach
    public void after() {
        Thread stopThread = new Thread(() -> KAFKA_BROKERS.destroy());
        stopThread.start();
        STOP_KAFKA_THREADS.add(stopThread);
    }

    @AfterAll
    public static void waitUntilKafka() throws InterruptedException {
        for(Thread t: STOP_KAFKA_THREADS) {
            t.join();
        }
    }

    @Test
    void testCallListener() throws InterruptedException, ExecutionException, IOException {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, TEST_GROUP_ID);

        Map<String, Object> kafkaProducerConfig = new HashMap<>();
        kafkaProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        CountDownLatch countDownLatch = new CountDownLatch(2);
        KafkaJdbcTransactionalListener.KafkaHandler<String, String> handler = (record, connection) -> {
            countDownLatch.countDown();
        };

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
             KafkaJdbcTransactionalListener<String, String> listener =
                     new KafkaJdbcTransactionalListener<>(Collections.singletonList(TEST_TOPIC), handler, DATASOURCE, kafkaConfig)) {

            listener.start();

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TEST_TOPIC, "Hello World");
            kafkaProducer.send(producerRecord).get();
            kafkaProducer.send(producerRecord).get();

            countDownLatch.await();
        }
    }

    @Test
    void testWithoutTransactionWithDatasource() throws InterruptedException {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, TEST_GROUP_ID);

        Map<String, Object> kafkaProducerConfig = new HashMap<>();
        kafkaProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        int numberOfMessages = NUMBER_OF_MESSAGE;
        CountDownLatch countDownLatch = new CountDownLatch(numberOfMessages);
        CountDownLatch startCountDownLatch = new CountDownLatch(1);

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
             Consumer<String, String> kafkaConsumer = new WaitingConsumer<>(new KafkaConsumer<>(kafkaConfig))) {

            kafkaConsumer.subscribe(Collections.singletonList(TEST_TOPIC));
            new Thread(() -> {
                while (countDownLatch.getCount() != 0) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                    startCountDownLatch.countDown();
                    for (ConsumerRecord<String, String> record: records) {
                        try (Connection connection = DATASOURCE.getConnection()
                        ) {
                            connection.setAutoCommit(false);
                            try(PreparedStatement statement =
                                        connection.prepareStatement("UPDATE TEST SET TID = ? WHERE ID = ?")) {
                                statement.setString(1, String.valueOf(record.offset()));
                                statement.setString(2, "1");
                                statement.executeUpdate();
                            }
                            connection.commit();
                        } catch (SQLException exception) {
                            exception.printStackTrace();
                        }
                        countDownLatch.countDown();
                    }
                }
            }).start();

            startCountDownLatch.await();

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TEST_TOPIC, "Hello World");
            for (int i = 0; i < numberOfMessages; i++) {
                kafkaProducer.send(producerRecord);
            }

            countDownLatch.await();
        }
    }

    @Test
    void testWithoutTransaction() throws InterruptedException {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, TEST_GROUP_ID);

        Map<String, Object> kafkaProducerConfig = new HashMap<>();
        kafkaProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        int numberOfMessages = NUMBER_OF_MESSAGE;
        CountDownLatch countDownLatch = new CountDownLatch(numberOfMessages);
        CountDownLatch startCountDownLatch = new CountDownLatch(1);

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
             Consumer<String, String> kafkaConsumer = new WaitingConsumer<>(new KafkaConsumer<>(kafkaConfig))) {

            kafkaConsumer.subscribe(Collections.singletonList(TEST_TOPIC));
            new Thread(() -> {
                while (countDownLatch.getCount() != 0) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                    startCountDownLatch.countDown();
                    for (ConsumerRecord<String, String> record: records) {
                        countDownLatch.countDown();
                    }
                }
            }).start();

            startCountDownLatch.await();

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TEST_TOPIC, "Hello World");
            for (int i = 0; i < numberOfMessages; i++) {
                kafkaProducer.send(producerRecord);
            }

            countDownLatch.await();
        }
    }

    @Test
    void testWithTransaction() throws InterruptedException, IOException {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, TEST_GROUP_ID);

        Map<String, Object> kafkaProducerConfig = new HashMap<>();
        kafkaProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        int numberOfMessages = NUMBER_OF_MESSAGE;
        CountDownLatch countDownLatch = new CountDownLatch(numberOfMessages);
        KafkaJdbcTransactionalListener.KafkaHandler<String, String> handler = (record, connection) -> {
            countDownLatch.countDown();
        };

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
             KafkaJdbcTransactionalListener<String, String> listener =
                     new KafkaJdbcTransactionalListener<>(Collections.singletonList(TEST_TOPIC), handler, DATASOURCE, kafkaConfig)) {

            listener.start();

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TEST_TOPIC, "Hello World");
            for (int i = 0; i < numberOfMessages; i++) {
                kafkaProducer.send(producerRecord);
            }

            countDownLatch.await();
        }
    }

    @Test
    public void testCallListenerRollBackOne() throws InterruptedException, ExecutionException, IOException {
        Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, TEST_GROUP_ID);

        Map<String, Object> kafkaProducerConfig = new HashMap<>();
        kafkaProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS.getBrokersAsString());
        kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        CountDownLatch countDownLatch = new CountDownLatch(3);
        AtomicInteger numberOfMessage = new AtomicInteger(0);
        KafkaJdbcTransactionalListener.KafkaHandler<String, String> handler = (record, connection) -> {
            countDownLatch.countDown();
            if (countDownLatch.getCount() == 1) {
                throw new RuntimeException("test");
            }
            numberOfMessage.incrementAndGet();
        };

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
             KafkaJdbcTransactionalListener<String, String> listener =
                     new KafkaJdbcTransactionalListener<>(Collections.singletonList(TEST_TOPIC), handler, DATASOURCE, kafkaConfig)) {
            listener.start();

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TEST_TOPIC, "Hello World 1");
            kafkaProducer.send(producerRecord).get();
            producerRecord = new ProducerRecord<>(TEST_TOPIC, "Hello World 2");
            kafkaProducer.send(producerRecord).get();

            countDownLatch.await();
        }

        CountDownLatch countDownLatchRestart = new CountDownLatch(3);
        handler = (record, connection) -> {
            countDownLatchRestart.countDown();
            numberOfMessage.incrementAndGet();
        };

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerConfig);
             KafkaJdbcTransactionalListener<String, String> listener =
                     new KafkaJdbcTransactionalListener<>(Collections.singletonList(TEST_TOPIC), handler, DATASOURCE, kafkaConfig)) {
            listener.start();

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TEST_TOPIC, "Hello World 1");
            kafkaProducer.send(producerRecord).get();

            countDownLatch.await(500, TimeUnit.MILLISECONDS);
        }

        assert numberOfMessage.get() == 3;
    }
}
