namespace SlimMessageBus.Host.AzureEventHub
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.Logging;
    using SlimMessageBus.Host.Config;

    public class EhGroupConsumer : IDisposable, IEventProcessorFactory
    {
        private readonly ILogger logger;

        public EventHubMessageBus MessageBus { get; }

        private readonly EventProcessorHost processorHost;
        private readonly Func<EhPartitionConsumer> partitionConsumerFactory;
        private readonly List<EhPartitionConsumer> partitionConsumers = new List<EhPartitionConsumer>();

        private readonly TaskMarker taskMarker = new TaskMarker();

        public EhGroupConsumer(EventHubMessageBus messageBus, [NotNull] ConsumerSettings consumerSettings)
            : this(messageBus, new TopicGroup(consumerSettings.Path, consumerSettings.GetGroup()), () => new EhPartitionConsumerForConsumers(messageBus, consumerSettings))
        {
        }

        public EhGroupConsumer(EventHubMessageBus messageBus, [NotNull] RequestResponseSettings requestResponseSettings)
            : this(messageBus, new TopicGroup(requestResponseSettings.Path, requestResponseSettings.GetGroup()), () => new EhPartitionConsumerForResponses(messageBus, requestResponseSettings))
        {
        }

        protected EhGroupConsumer(EventHubMessageBus messageBus, TopicGroup topicGroup, Func<EhPartitionConsumer> partitionConsumerFactory)
        {
            if (topicGroup is null) throw new ArgumentNullException(nameof(topicGroup));

            MessageBus = messageBus ?? throw new ArgumentNullException(nameof(messageBus));
            logger = messageBus.LoggerFactory.CreateLogger<EhGroupConsumer>();
            this.partitionConsumerFactory = partitionConsumerFactory ?? throw new ArgumentNullException(nameof(partitionConsumerFactory));

            logger.LogInformation("Creating EventProcessorHost for EventHub with Path: {Path}, Group: {Group}", topicGroup.Topic, topicGroup.Group);
            processorHost = MessageBus.ProviderSettings.EventProcessorHostFactory(topicGroup);

            var eventProcessorOptions = MessageBus.ProviderSettings.EventProcessorOptionsFactory(topicGroup);
            processorHost.RegisterEventProcessorFactoryAsync(this, eventProcessorOptions).Wait();
        }

        #region Implementation of IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                processorHost.UnregisterEventProcessorAsync().Wait();

                taskMarker.Stop().Wait();

                if (partitionConsumers.Count > 0)
                {
                    partitionConsumers.ForEach(ep => ep.DisposeSilently("EventProcessor", logger));
                    partitionConsumers.Clear();
                }
            }
        }

        #endregion

        #region Implementation of IEventProcessorFactory

        public IEventProcessor CreateEventProcessor([NotNull] PartitionContext context)
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug("Creating IEventProcessor for Path: {Path}, PartitionId: {PartitionId}, Offset: {Offset}", context.EventHubPath, context.PartitionId, context.Lease.Offset);
            }

            var ep = partitionConsumerFactory();
            partitionConsumers.Add(ep);
            return ep;
        }

        #endregion
    }
}