namespace SlimMessageBus.Host.Kafka
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Microsoft.Extensions.Logging;
    using SlimMessageBus.Host.Config;
    using ConsumeResult = Confluent.Kafka.ConsumeResult<Confluent.Kafka.Ignore, byte[]>;

    public abstract class KafkaPartitionConsumer : IKafkaPartitionConsumer
    {
        private readonly ILogger logger;

        private readonly MessageBusBase messageBus;
        private readonly AbstractConsumerSettings consumerSettings;
        private readonly IKafkaCommitController commitController;
        private readonly IMessageProcessor<ConsumeResult> messageProcessor;
        private readonly ICheckpointTrigger checkpointTrigger;

        private TopicPartitionOffset lastOffset;
        private TopicPartitionOffset lastCheckpointOffset;

        protected KafkaPartitionConsumer(AbstractConsumerSettings consumerSettings, TopicPartition topicPartition, IKafkaCommitController commitController, MessageBusBase messageBus, IMessageProcessor<ConsumeResult> messageProcessor)
        {
            this.messageBus = messageBus ?? throw new ArgumentNullException(nameof(messageBus));

            logger = this.messageBus.LoggerFactory.CreateLogger<KafkaPartitionConsumerForConsumers>();
            logger.LogInformation("Creating consumer for Group: {Group}, Topic: {Topic}, Partition: {Partition}", consumerSettings.GetGroup(), consumerSettings.Path, topicPartition.Partition);

            TopicPartition = topicPartition;

            this.consumerSettings = consumerSettings;
            this.commitController = commitController;
            this.messageProcessor = messageProcessor;
            // ToDo: Add support for Kafka driven automatic commit
            this.checkpointTrigger = new CheckpointTrigger(consumerSettings);
        }

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                messageProcessor.Dispose();
            }
        }

        #endregion

        #region Implementation of IKafkaTopicPartitionProcessor

        public TopicPartition TopicPartition { get; }

        public void OnPartitionAssigned([NotNull] TopicPartition partition)
        {
            if (checkpointTrigger != null)
            {
                checkpointTrigger.Reset();
            }
        }

        public async Task OnMessage([NotNull] ConsumeResult message)
        {
            try
            {
                lastOffset = message.TopicPartitionOffset;

                var lastException = await messageProcessor.ProcessMessage(message, consumerInvoker: null).ConfigureAwait(false);
                if (lastException != null)
                {
                    // ToDo: Retry logic
                    // The OnMessageFaulted was called at this point by the MessageProcessor.
                }

                if (checkpointTrigger != null && checkpointTrigger.Increment())
                {
                    checkpointTrigger.Reset();

                    Commit(message.TopicPartitionOffset);
                }
            }
            catch (Exception e)
            {
                logger.LogError(e, "Group [{Group}]: Error occured while consuming a message at Topic: {Topic}, Partition: {Partition}, Offset: {Offset}", consumerSettings.GetGroup(), message.Topic, message.Partition, message.Offset);
                throw;
            }
        }

        public void OnPartitionEndReached(TopicPartitionOffset offset)
        {
            if (checkpointTrigger != null)
            {
                if (offset != null && (lastCheckpointOffset == null || offset.Offset > lastCheckpointOffset.Offset))
                {
                    Commit(offset);
                }
                checkpointTrigger.Reset();
            }
        }

        public void OnPartitionRevoked()
        {
            if (checkpointTrigger != null)
            {
                OnPartitionEndReached(lastOffset);

                lastCheckpointOffset = null;
                lastOffset = null;
            }
        }

        public void OnClose()
        {
            if (checkpointTrigger != null)
            {
                Commit(lastOffset);
            }
        }

        #endregion

        public void Commit(TopicPartitionOffset offset)
        {
            logger.LogDebug("Group [{Group}]: Will commit at Topic: {Topic}, Partition: {Partition}, Offset: {Offset}", consumerSettings.GetGroup(), offset.Topic, offset.Partition, offset.Offset);

            lastCheckpointOffset = offset;
            commitController.Commit(offset);
        }
    }
}