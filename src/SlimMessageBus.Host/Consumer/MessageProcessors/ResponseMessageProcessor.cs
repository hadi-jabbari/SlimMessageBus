namespace SlimMessageBus.Host
{
    using System;
    using System.Threading.Tasks;
    using SlimMessageBus.Host.Config;

    /// <summary>
    /// The <see cref="IMessageProcessor{TMessage}"/> implementation that processes the responses arriving to the bus.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    public class ResponseMessageProcessor<TMessage> : IMessageProcessor<TMessage> where TMessage : class
    {
        private readonly RequestResponseSettings requestResponseSettings;
        private readonly MessageBusBase messageBus;
        private readonly Func<TMessage, MessageWithHeaders> messageProvider;

        public ResponseMessageProcessor(RequestResponseSettings requestResponseSettings, MessageBusBase messageBus, Func<TMessage, MessageWithHeaders> messageProvider)
        {
            this.requestResponseSettings = requestResponseSettings ?? throw new ArgumentNullException(nameof(requestResponseSettings));
            this.messageBus = messageBus ?? throw new ArgumentNullException(nameof(messageBus));
            this.messageProvider = messageProvider ?? throw new ArgumentNullException(nameof(messageProvider));
        }

        public AbstractConsumerSettings ConsumerSettings => requestResponseSettings;

        public Task<Exception> ProcessMessage(TMessage message, IMessageTypeConsumerInvokerSettings consumerInvoker)
        {
            var messageWithHeaders = messageProvider(message);
            return messageBus.OnResponseArrived(messageWithHeaders.Payload, requestResponseSettings.Path, messageWithHeaders.Headers);
        }

        #region IDisposable

        protected virtual void Dispose(bool disposing)
        {
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}