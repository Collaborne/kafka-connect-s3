package com.spredfast.kafka.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.ref.Reference;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingServer;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreateableTopicConfigCollection;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.AlterConfigsRequest.Config;
import org.apache.kafka.common.requests.AlterConfigsRequest.ConfigEntry;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import kafka.admin.AdminUtils;
import kafka.server.AdminManager;
import kafka.server.BrokerState;
import kafka.server.BrokerStates;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.RunningAsBroker;
import scala.Option;
import scala.collection.JavaConverters;

public class KafkaIntegrationTests {

	private static final int SLEEP_INTERVAL = 300;

	public static Kafka givenLocalKafka() throws Exception {
		return new Kafka();
	}

	public static void givenLocalKafka(int kafkaPort, IntConsumer localPort) throws Exception {
		try (Kafka kafka = givenLocalKafka()) {
			localPort.accept(kafka.localPort());
		}
	}

	public static void givenKafkaConnect(int kafkaPort, Consumer<Herder> consumer) throws Exception {
		try (KafkaConnect connect = givenKafkaConnect(kafkaPort)) {
			consumer.accept(connect.herder());
		}
	}

	public static void waitForPassing(Duration timeout, Runnable test) {
		waitForPassing(timeout, () -> {
			test.run();
			return null;
		});
	}

	public static <T> T waitForPassing(Duration timeout, Callable<T> test) {
		AssertionError last = null;
		for (int i = 0; i < timeout.toMillis() / SLEEP_INTERVAL; i++) {
			try {
				return test.call();
			} catch (AssertionError e) {
				last = e;
				try {
					Thread.sleep(SLEEP_INTERVAL);
				} catch (InterruptedException e1) {
					Throwables.propagate(e1);
				}
			} catch (Exception e) {
				Throwables.propagate(e);
			}
		}
		if (last != null) {
			throw last;
		}
		return null;
	}

	public static KafkaConnect givenKafkaConnect(int kafkaPort) throws IOException {
		return givenKafkaConnect(kafkaPort, ImmutableMap.of());
	}

	public static KafkaConnect givenKafkaConnect(int kafkaPort, Map<? extends String, ? extends String> overrides) throws IOException {
		File tempFile = File.createTempFile("connect", "offsets");
		System.err.println("Storing offsets at " + tempFile);
		HashMap<String, String> props = new HashMap<>(ImmutableMap.<String, String>builder()
			.put("bootstrap.servers", "localhost:" + kafkaPort)
			// perform no conversion
			.put("key.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter")
			.put("value.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter")
			.put("internal.key.converter", QuietJsonConverter.class.getName())
			.put("internal.value.converter", QuietJsonConverter.class.getName())
			.put("internal.key.converter.schemas.enable", "true")
			.put("internal.value.converter.schemas.enable", "true")
			.put("offset.storage.file.filename", tempFile.getCanonicalPath())
			.put("offset.flush.interval.ms", "1000")
			.put("consumer.metadata.max.age.ms", "1000")
			.put("rest.port", "" + InstanceSpec.getRandomPort())
			.build()
		);
		props.putAll(overrides);

		return givenKafkaConnect(props);
	}

	private static KafkaConnect givenKafkaConnect(Map<String, String> props) {
		WorkerConfig config = new StandaloneConfig(props);
		ConnectorClientConfigOverridePolicy policy = new NoneConnectorClientConfigOverridePolicy();
		Worker worker = new Worker("1", new SystemTime(), new Plugins(Collections.emptyMap()), config, new FileOffsetBackingStore(), policy);
		Herder herder = new StandaloneHerder(worker, "clusterId", policy);
		RestServer restServer = new RestServer(config);
		Connect connect = new Connect(herder, restServer);
		connect.start();
		return new KafkaConnect(connect, herder, () -> givenKafkaConnect(props));
	}

	public static class KafkaConnect implements AutoCloseable {

		private final Connect connect;
		private final Herder herder;
		private final Supplier<KafkaConnect> restart;

		public KafkaConnect(Connect connect, Herder herder, Supplier<KafkaConnect> restart) {
			this.connect = connect;
			this.herder = herder;
			this.restart = restart;
		}

		@Override
		public void close() throws Exception {
			connect.stop();
			connect.awaitStop();
		}

		public KafkaConnect restart() {
			return restart.get();
		}

		public Herder herder() {
			return herder;
		}
	}

	public static class Kafka implements AutoCloseable {
		private final TestingServer zk;
		private final KafkaServer kafkaServer;

		public Kafka() throws Exception {
			zk = new TestingServer();
			File tmpDir = Files.createTempDir();
			KafkaConfig config = new KafkaConfig(Maps.transformValues(ImmutableMap.<String, Object>builder()
				.put("port", InstanceSpec.getRandomPort())
				.put("broker.id", "1")
				.put("offsets.topic.replication.factor", 1)
				.put("log.dir", tmpDir.getCanonicalPath())
				.put("zookeeper.connect", zk.getConnectString())
				.build(), Functions.toStringFunction()));
			kafkaServer = new KafkaServer(config, SystemTime.SYSTEM, Option.empty(), JavaConverters.asScala(ImmutableList.of()));
			kafkaServer.startup();
		}

		public int localPort() {
			return kafkaServer.config().advertisedPort();
		}

		@Override
		public void close() throws Exception {
			kafkaServer.shutdown();
			kafkaServer.awaitShutdown();
			zk.close();
		}

		public String createUniqueTopic(String prefix) throws InterruptedException {
			return createUniqueTopic(prefix, 1);
		}

		public String createUniqueTopic(String prefix, int partitions) throws InterruptedException {
			return createUniqueTopic(prefix, partitions, new Properties());
		}

		public String createUniqueTopic(String prefix, int partitions, Properties topicConfig) throws InterruptedException {
			checkReady();
			String topicName = (prefix + UUID.randomUUID().toString().substring(0, 5)).replaceAll("[^a-zA-Z0-9._-]", "_");
			List<CreateableTopicConfig> configs = topicConfig
				.entrySet().stream()
				.map(entry -> new CreateableTopicConfig().setName(entry.getKey().toString()).setValue(entry.getValue().toString()))
				.collect(Collectors.toList());
			CreatableTopic topic = new CreatableTopic()
				.setName(topicName)
				.setNumPartitions(1)
				.setConfigs(new CreateableTopicConfigCollection(configs.iterator()));
			AtomicReference<ApiError> creationError = new AtomicReference<>();
			kafkaServer.adminManager().createTopics(
				(int) Duration.ofSeconds(5).toMillis(), false,
				JavaConverters.asScala(Collections.singletonMap(topicName, topic)),
				JavaConverters.asScala(Collections.emptyMap()),
				results -> {
					creationError.set(results.get(topicName).get());
					return null;
				});
			waitForPassing(Duration.ofSeconds(10), () -> {
				assertNotNull(creationError.get());
				assertTrue(creationError.get().isSuccess());
			});
			return topicName;
		}

		public void updateTopic(String topic, Properties topicConfig) {
			List<ConfigEntry> configEntries = topicConfig
				.entrySet().stream()
				.map(entry -> new ConfigEntry(entry.getKey().toString(), entry.getValue().toString()))
				.collect(Collectors.toList());
			Config config = new Config(configEntries);
			AdminManager adminManager = kafkaServer.adminManager();
			adminManager.alterConfigs(JavaConverters.asScala(Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, topic), config)), false);
		}

		public void checkReady() throws InterruptedException {
			checkReady(Duration.ofSeconds(15));
		}

		public void checkReady(Duration timeout) throws InterruptedException {
			waitForPassing(timeout, () -> assertNotNull(kafkaServer.brokerState().currentState() == RunningAsBroker.state()));
		}
	}

}
