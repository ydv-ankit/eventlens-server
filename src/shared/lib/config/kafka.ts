import { Kafka } from "kafkajs";
import { ENV } from "@/shared/utils/env";
import logger from "@/shared/utils/logger";
import {
  KAFKA_CONSUMER_GROUP,
  KAFKA_RETRY_TOPIC,
  KAFKA_TOPIC,
  KAFKA_TOPIC_PARTITIONS,
} from "@/shared/utils/constants";

const kafkaClient = new Kafka({
  clientId: ENV.KAFKA_CLIENT_ID || "eventlens-app",
  brokers: (ENV.KAFKA_BROKERS || "kafka:9092")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean),
});

const kafkaProducer = kafkaClient.producer();
let kafkaProducerConnected = false;

const KAFKA_RECONNECT_INTERVAL_MS = 5000;
const configuredTopicPartitions =
  ENV.KAFKA_TOPIC_PARTITIONS || KAFKA_TOPIC_PARTITIONS;

const ensureKafkaTopics = async () => {
  const admin = kafkaClient.admin();
  try {
    await admin.connect();
    const existing = new Set(await admin.listTopics());
    const desired = [
      ENV.KAFKA_TOPIC || KAFKA_TOPIC,
      ENV.KAFKA_RETRY_TOPIC || KAFKA_RETRY_TOPIC,
    ];
    const toCreate = desired.filter((t) => !existing.has(t));
    if (toCreate.length > 0) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: toCreate.map((topic) => ({
          topic,
          numPartitions: configuredTopicPartitions,
          replicationFactor: 1,
        })),
      });
    }

    for (const topic of desired) {
      try {
        const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
        const topicMeta = metadata.topics.find((t) => t.name === topic);
        if (topicMeta && topicMeta.partitions.length < configuredTopicPartitions) {
          await admin.createPartitions({
            topicPartitions: [{ topic, count: configuredTopicPartitions }],
          });
          logger.debug(`Kafka topic ${topic} partitions increased to ${configuredTopicPartitions}`);
        }
      } catch (err) {
        logger.error(`Kafka partition sync failed for ${topic}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger.error("Kafka topic initialization failed: " + message);
  } finally {
    await admin.disconnect();
  }
};

const connectKafkaProducer = async () => {
  if (kafkaProducerConnected) {
    return;
  }

  try {
    await ensureKafkaTopics();
    await kafkaProducer.connect();
    kafkaProducerConnected = true;
    logger.debug("Kafka producer connected");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger.error("Kafka producer connect failed: " + message);
    setTimeout(connectKafkaProducer, KAFKA_RECONNECT_INTERVAL_MS);
  }
};

const isKafkaProducerConnected = () => kafkaProducerConnected;

const getKafkaConsumer = (groupId = ENV.KAFKA_CONSUMER_GROUP || KAFKA_CONSUMER_GROUP) =>
  kafkaClient.consumer({ groupId });

const subscribeKafkaConsumer = async (
  consumer: ReturnType<typeof getKafkaConsumer>,
  topic = ENV.KAFKA_TOPIC || KAFKA_TOPIC,
) => {
  await consumer.subscribe({
    topic,
    fromBeginning: false,
  });
};

export {
  connectKafkaProducer,
  ensureKafkaTopics,
  getKafkaConsumer,
  isKafkaProducerConnected,
  kafkaClient,
  kafkaProducer,
  subscribeKafkaConsumer,
};
