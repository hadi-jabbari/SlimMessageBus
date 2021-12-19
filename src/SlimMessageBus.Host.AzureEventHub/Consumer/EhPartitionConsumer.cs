namespace SlimMessageBus.Host.AzureEventHub
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.Logging;
    using SlimMessageBus.Host.Config;

    public abstract class EhPartitionConsumer : IEventProcessor, IDisposable
    {
        private readonly ILogger logger;
        protected EventHubMessageBus MessageBus { get; }
        protected TaskMarker TaskMarker { get; } = new TaskMarker();
        protected ICheckpointTrigger CheckpointTrigger { get; }
        protected AbstractConsumerSettings ConsumerSettings { get; }
        protected IMessageProcessor<EventData> MessageProcessor { get; }

        private EventData lastMessage;
        private EventData lastCheckpointMessage;

        protected EhPartitionConsumer(EventHubMessageBus messageBus, AbstractConsumerSettings consumerSettings, IMessageProcessor<EventData> messageProcessor)
        {
            MessageBus = messageBus ?? throw new ArgumentNullException(nameof(messageBus));
            logger = messageBus.LoggerFactory.CreateLogger<EhPartitionConsumer>();
            // ToDo: Make the checkpoint optional - let EH control when to commit
            CheckpointTrigger = new CheckpointTrigger(consumerSettings);
            ConsumerSettings = consumerSettings;
            MessageProcessor = messageProcessor;
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
                TaskMarker.Stop().Wait();
            }
        }

        #endregion

        #region Implementation of IEventProcessor

        public Task OpenAsync([NotNull] PartitionContext context)
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug("Open lease: {0}", new PartitionContextInfo(context));
            }

            CheckpointTrigger?.Reset();

            return Task.CompletedTask;
        }

        public async Task ProcessEventsAsync([NotNull] PartitionContext context, [NotNull] IEnumerable<EventData> messages)
        {
            TaskMarker.OnStarted();
            try
            {
                foreach (var message in messages)
                {
                    if (!TaskMarker.CanRun)
                    {
                        break;
                    }

                    lastMessage = message;

                    var lastException = await MessageProcessor.ProcessMessage(message, consumerInvoker: null).ConfigureAwait(false);
                    if (lastException != null)
                    {
                        // ToDo: Retry logic
                        // The OnMessageFaulted was called at this point by the MessageProcessor.
                    }
                    if (CheckpointTrigger != null && CheckpointTrigger.Increment())
                    {
                        CheckpointTrigger.Reset();

                        lastCheckpointMessage = message;
                        await Checkpoint(message, context).ConfigureAwait(false);
                        if (!ReferenceEquals(lastCheckpointMessage, message))
                        {
                            // something went wrong (not all messages were processed with success)

                            // ToDo: add retry support
                            //skipLastCheckpoint = !ReferenceEquals(lastCheckpointMessage, message);
                            //skipLastCheckpoint = false;
                        }
                    }
                }
            }
            finally
            {
                TaskMarker.OnFinished();
            }
        }

        public Task ProcessErrorAsync([NotNull] PartitionContext context, Exception error)
        {
            // ToDo: improve error handling
            logger.LogError(error, "Partition {0} error", new PartitionContextInfo(context));
            return Task.CompletedTask;
        }

        public Task CloseAsync([NotNull] PartitionContext context, CloseReason reason)
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug("Close lease: Reason: {0}, {1}", reason, new PartitionContextInfo(context));
            }

            if (CheckpointTrigger != null)
            {
                // checkpoint the last messages
                if (lastMessage != null && !ReferenceEquals(lastCheckpointMessage, lastMessage))
                {
                    return Checkpoint(lastMessage, context);
                }
            }

            return Task.CompletedTask;
        }

        #endregion        

        private Task Checkpoint(EventData message, PartitionContext context)
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug("Will checkpoint at Offset: {0}, {1}", message.SystemProperties.Offset, new PartitionContextInfo(context));
            }
            if (message != null)
            {
                return context.CheckpointAsync(message);
            }
            return Task.CompletedTask;
        }

        protected static MessageWithHeaders GetMessageWithHeaders(EventData e) => new MessageWithHeaders(e.Body.Array, e.Properties);
    }
}