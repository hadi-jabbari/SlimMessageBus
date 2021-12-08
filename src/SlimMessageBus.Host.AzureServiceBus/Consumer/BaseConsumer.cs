namespace SlimMessageBus.Host.AzureServiceBus.Consumer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Microsoft.Extensions.Logging;
    using SlimMessageBus.Host.Config;

    public abstract class BaseConsumer : IDisposable
    {
        private readonly ILogger logger;
        public ServiceBusMessageBus MessageBus { get; }
        protected IReceiverClient Client { get; }
        protected IList<ConsumerRegistration> Consumers { get; }
        protected IDictionary<Type, (ConsumerSettings Settings, IMessageProcessor<Message> Processor, IMessageTypeConsumerInvokerSettings Invoker)> InvokerByMessageType { get; }

        protected BaseConsumer(ServiceBusMessageBus messageBus, IReceiverClient client, IEnumerable<ConsumerRegistration> consumers, string pathName, ILogger logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            MessageBus = messageBus ?? throw new ArgumentNullException(nameof(messageBus));
            Client = client ?? throw new ArgumentNullException(nameof(client));
            Consumers = consumers?.ToList() ?? throw new ArgumentNullException(nameof(consumers));

            if (Consumers.Count == 0)
            {
                throw new InvalidOperationException($"The {nameof(consumers)} needs to be non empty");
            }

            var instances = Consumers.First().Settings.Instances;
            if (Consumers.Any(x => x.Settings.Instances != instances))
            {
                throw new ConfigurationMessageBusException($"All declared consumers across the same path/subscription {pathName} must have the same Instances settings.");
            }

            InvokerByMessageType = Consumers
                .Where(x => x.Settings is ConsumerSettings)
                .SelectMany(x => ((ConsumerSettings)x.Settings).ConsumersByMessageType.Values.Select(invoker => (Settings: (ConsumerSettings)x.Settings, x.Processor, Invoker: invoker)))
                .ToDictionary(x => x.Invoker.MessageType);

            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = instances,

                // Indicates whether the message pump should automatically complete the messages after returning from user callback.
                // False below indicates the complete operation is handled by the user callback as in ProcessMessagesAsync().
                AutoComplete = false
            };

            // Register the function that processes messages.
            Client.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        #region IDisposable

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                foreach (var messageProcessor in Consumers.Select(x => x.Processor))
                {
                    messageProcessor.DisposeSilently();
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        protected virtual ConsumerRegistration TryMatchConsumer(Type messageType)
        {
            if (messageType != null)
            {
                // Find proper Consumer from Consumers based on the incoming message type
                while (messageType.BaseType != typeof(object))
                {
                    InvokerByMessageType
                }
            }

            // fallback to the first one
            return Consumers[0];
        }

        protected async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            if (message is null) throw new ArgumentNullException(nameof(message));

            var messageType = GetMessageType(message);
            var consumer = TryMatchConsumer(messageType);

            // Process the message.
            var mf = consumer.Settings.FormatIf(message, logger.IsEnabled(LogLevel.Debug));
            logger.LogDebug("Received message - {0}", mf);

            if (token.IsCancellationRequested)
            {
                // Note: Use the cancellationToken passed as necessary to determine if the subscriptionClient has already been closed.
                // If subscriptionClient has already been closed, you can choose to not call CompleteAsync() or AbandonAsync() etc.
                // to avoid unnecessary exceptions.
                logger.LogDebug("Abandon message - {0}", mf);
                await Client.AbandonAsync(message.SystemProperties.LockToken).ConfigureAwait(false);

                return;
            }

            var exception = await consumer.Processor.ProcessMessage(message).ConfigureAwait(false);
            if (exception != null)
            {
                if (mf == null)
                {
                    mf = consumer.Settings.FormatIf(message, true);
                }
                logger.LogError(exception, "Abandon message (exception occured while processing) - {0}", mf);

                try
                {
                    // Execute the event hook
                    consumer.Settings.OnMessageFault?.Invoke(MessageBus, consumer.Settings, null, exception, message);
                    MessageBus.Settings.OnMessageFault?.Invoke(MessageBus, consumer.Settings, null, exception, message);
                }
                catch (Exception eh)
                {
                    MessageBusBase.HookFailed(logger, eh, nameof(IConsumerEvents.OnMessageFault));
                }

                var messageProperties = new Dictionary<string, object>
                {
                    // Set the exception message
                    ["SMB.Exception"] = exception.Message
                };
                await Client.AbandonAsync(message.SystemProperties.LockToken, messageProperties).ConfigureAwait(false);

                return;
            }

            // Complete the message so that it is not received again.
            // This can be done only if the subscriptionClient is created in ReceiveMode.PeekLock mode (which is the default).
            logger.LogDebug("Complete message - {0}", mf);
            await Client.CompleteAsync(message.SystemProperties.LockToken).ConfigureAwait(false);
        }

        protected Type GetMessageType(Message message)
        {
            if (message != null && !message.UserProperties.TryGetValue(MessageHeaders.MessageType, out var messageTypeValue) && messageTypeValue is string messageTypeName)
            {
                var messageType = MessageBus.Settings.MessageTypeResolver.ToType(messageTypeName);
                return messageType;
            }
            return null;
        }

        // Use this handler to examine the exceptions received on the message pump.
        protected Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            try
            {
                // Execute the event hook
                MessageBus.Settings.OnMessageFault?.Invoke(MessageBus, null, null, exceptionReceivedEventArgs?.Exception, null);
            }
            catch (Exception eh)
            {
                MessageBusBase.HookFailed(logger, eh, nameof(IConsumerEvents.OnMessageFault));
            }
            return Task.CompletedTask;
        }
    }
}