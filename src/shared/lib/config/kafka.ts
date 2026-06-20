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
const kafkaAdmin = kafkaClient.admin();
let kafkaProducerConnected = false;

const KAFKA_RECONNECT_INTERVAL_MS = 5000;
const configuredTopicPartitions =
  ENV.KAFKA_TOPIC_PARTITIONS || KAFKA_TOPIC_PARTITIONS;

const ensureTopicPartitions = async (topic: string) => {
  try {
    const metadata = await kafkaAdmin.fetchTopicMetadata({ topics: [topic] });
    const topicMetadata = metadata.topics.find((item) => item.name === topic);
    if (!topicMetadata) {
      return;
    }

    const currentPartitions = topicMetadata.partitions.length;
    if (currentPartitions < configuredTopicPartitions) {
      await kafkaAdmin.createPartitions({
        topicPartitions: [
          {
            topic,
            count: configuredTopicPartitions,
          },
        ],
      });
      logger.debug(
        `Kafka topic ${topic} partitions increased from ${currentPartitions} to ${configuredTopicPartitions}`,
      );
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger.error(`Kafka partition sync failed for ${topic}: ${message}`);
  }
};

const ensureKafkaTopics = async () => {
  try {
    await kafkaAdmin.connect();
    await kafkaAdmin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: ENV.KAFKA_TOPIC || KAFKA_TOPIC,
          numPartitions: configuredTopicPartitions,
          replicationFactor: 1,
        },
        {
          topic: ENV.KAFKA_RETRY_TOPIC || KAFKA_RETRY_TOPIC,
          numPartitions: configuredTopicPartitions,
          replicationFactor: 1,
        },
      ],
    });

    await ensureTopicPartitions(ENV.KAFKA_TOPIC || KAFKA_TOPIC);
    await ensureTopicPartitions(ENV.KAFKA_RETRY_TOPIC || KAFKA_RETRY_TOPIC);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger.error("Kafka topic initialization failed: " + message);
  } finally {
    await kafkaAdmin.disconnect();
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
