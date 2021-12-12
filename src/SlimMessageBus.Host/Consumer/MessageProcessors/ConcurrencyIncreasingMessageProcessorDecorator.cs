namespace SlimMessageBus.Host
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using SlimMessageBus.Host.Config;

    /// <summary>
    /// Decorator for <see cref="IMessageProcessor{TMessage}"> that increases the amount of messages being concurrently processed.
    /// The expectation is that <see cref="IMessageProcessor{TMessage}.ProcessMessage(TMessage)"/> will be executed synchronously (in sequential order) by the caller on which we want to increase amount of concurrent message being processed.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    public class ConcurrencyIncreasingMessageProcessorDecorator<TMessage> : IMessageProcessor<TMessage> where TMessage : class
    {
        private readonly ILogger logger;
        private readonly SemaphoreSlim concurrentSemaphore;
        private readonly IMessageProcessor<TMessage> target;
        private Exception lastException;
        private int pendingCount;

        public int PendingCount => pendingCount;

        public ConcurrencyIncreasingMessageProcessorDecorator(AbstractConsumerSettings consumerSettings, MessageBusBase messageBus, IMessageProcessor<TMessage> target)
        {
            if (consumerSettings is null) throw new ArgumentNullException(nameof(consumerSettings));
            if (messageBus is null) throw new ArgumentNullException(nameof(messageBus));

            logger = messageBus.LoggerFactory.CreateLogger<ConsumerInstancePoolMessageProcessor<TMessage>>();
            concurrentSemaphore = new SemaphoreSlim(consumerSettings.Instances);
            this.target = target;
        }

        public AbstractConsumerSettings ConsumerSettings => target.ConsumerSettings;

        public async Task<Exception> ProcessMessage(TMessage message, IMessageTypeConsumerInvokerSettings consumerInvoker)
        {
            // Ensure only desired number of messages are being processed concurrently
            await concurrentSemaphore.WaitAsync().ConfigureAwait(false);

            // Check if there was an exception from and earlier message processing
            var e = lastException;
            if (e != null)
            {
                // report the last exception
                lastException = null;
                return e;
            }

            Interlocked.Increment(ref pendingCount);
            // Fire and forget
            _ = ProcessInBackground(message, consumerInvoker);

            // Not exception - we don't know yet
            return null;
        }

        private async Task ProcessInBackground(TMessage message, IMessageTypeConsumerInvokerSettings consumerInvoker)
        {
            try
            {
                logger.LogDebug("Entering ProcessMessages for message {MessageType}", typeof(TMessage));
                var exception = await target.ProcessMessage(message, consumerInvoker).ConfigureAwait(false);
                if (exception != null)
                {
                    lastException = exception;
                }
            }
            finally
            {
                logger.LogDebug("Leaving ProcessMessages for message {MessageType}", typeof(TMessage));
                concurrentSemaphore.Release();

                Interlocked.Decrement(ref pendingCount);
            }
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
                concurrentSemaphore.Dispose();
                target.Dispose();
            }
        }

        #endregion
    }
}