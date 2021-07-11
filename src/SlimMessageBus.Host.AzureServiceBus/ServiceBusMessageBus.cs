﻿namespace SlimMessageBus.Host.AzureServiceBus
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Extensions.Logging;
    using SlimMessageBus.Host.AzureServiceBus.Consumer;
    using SlimMessageBus.Host.Collections;
    using SlimMessageBus.Host.Config;

    public class ServiceBusMessageBus : MessageBusBase
    {
        private readonly ILogger _logger;

        public ServiceBusMessageBusSettings ProviderSettings { get; }

        private SafeDictionaryWrapper<string, ITopicClient> _producerByTopic;
        private SafeDictionaryWrapper<string, IQueueClient> _producerByQueue;

        private readonly KindMapping _kindMapping = new KindMapping();

        private readonly List<BaseConsumer> _consumers = new List<BaseConsumer>();

        public ServiceBusMessageBus(MessageBusSettings settings, ServiceBusMessageBusSettings providerSettings)
            : base(settings)
        {
            _logger = LoggerFactory.CreateLogger<ServiceBusMessageBus>();
            ProviderSettings = providerSettings ?? throw new ArgumentNullException(nameof(providerSettings));

            OnBuildProvider();
        }

        protected override void AssertConsumerSettings(ConsumerSettings consumerSettings)
        {
            if (consumerSettings is null) throw new ArgumentNullException(nameof(consumerSettings));

            base.AssertConsumerSettings(consumerSettings);

            Assert.IsTrue(consumerSettings.PathKind != PathKind.Topic || consumerSettings.GetSubscriptionName(required: false) != null,
                () => new ConfigurationMessageBusException($"The {nameof(ConsumerSettings)}.{nameof(SettingsExtensions.SubscriptionName)} is not set on topic {consumerSettings.Path}"));
        }

        protected void AddConsumer(AbstractConsumerSettings consumerSettings, IMessageProcessor<Message> messageProcessor)
        {
            if (consumerSettings is null) throw new ArgumentNullException(nameof(consumerSettings));

            var consumer = consumerSettings.PathKind == PathKind.Topic
                ? new TopicSubscriptionConsumer(this, consumerSettings, messageProcessor) as BaseConsumer
                : new QueueConsumer(this, consumerSettings, messageProcessor);

            _consumers.Add(consumer);
        }

        #region Overrides of MessageBusBase

        protected override void Build()
        {
            base.Build();

            _producerByTopic = new SafeDictionaryWrapper<string, ITopicClient>(topic =>
            {
                _logger.LogDebug("Creating {0} for name {1}", nameof(ITopicClient), topic);
                return ProviderSettings.TopicClientFactory(topic);
            });

            _producerByQueue = new SafeDictionaryWrapper<string, IQueueClient>(queue =>
            {
                _logger.LogDebug("Creating {0} for name {1}", nameof(IQueueClient), queue);
                return ProviderSettings.QueueClientFactory(queue);
            });

            _kindMapping.Configure(Settings);

            byte[] getPayload(Message m) => m.Body;
            void initConsumerContext(Message m, ConsumerContext ctx) => ctx.SetTransportMessage(m);

            _logger.LogInformation("Creating consumers");
            foreach (var consumerSettings in Settings.Consumers)
            {
                _logger.LogInformation("Creating consumer for {0}", consumerSettings.FormatIf(_logger.IsEnabled(LogLevel.Information)));

                AddConsumer(consumerSettings, new ConsumerInstanceMessageProcessor<Message>(consumerSettings, this, getPayload, initConsumerContext));
            }

            if (Settings.RequestResponse != null)
            {
                _logger.LogInformation("Creating response consumer for {0}", Settings.RequestResponse.FormatIf(_logger.IsEnabled(LogLevel.Information)));

                AddConsumer(Settings.RequestResponse, new ResponseMessageProcessor<Message>(Settings.RequestResponse, this, getPayload));
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (_consumers.Count > 0)
            {
                _consumers.ForEach(c => c.DisposeSilently("Consumer", _logger));
                _consumers.Clear();
            }

            var disposeTasks = Enumerable.Empty<Task>();

            if (_producerByQueue.Dictonary.Count > 0)
            {
                disposeTasks = disposeTasks.Concat(_producerByQueue.Dictonary.Values.Select(x =>
                {
                    _logger.LogDebug("Closing {0} for name {1}", nameof(IQueueClient), x.Path);
                    return x.CloseAsync();
                }));

                _producerByQueue.Clear();
            }

            if (_producerByTopic.Dictonary.Count > 0)
            {
                disposeTasks = disposeTasks.Concat(_producerByTopic.Dictonary.Values.Select(x =>
                {
                    _logger.LogDebug("Closing {0} for name {1}", nameof(ITopicClient), x.Path);
                    return x.CloseAsync();
                }));

                _producerByTopic.Clear();
            }

            Task.WaitAll(disposeTasks.ToArray());

            base.Dispose(disposing);
        }

        protected virtual async Task ProduceToTransport(Type messageType, object message, string name, byte[] payload, PathKind kind)
        {
            if (messageType is null) throw new ArgumentNullException(nameof(messageType));
            if (payload is null) throw new ArgumentNullException(nameof(payload));

            AssertActive();

            _logger.LogDebug("Producing message {0} of type {1} on {2} {3} with size {4}", message, messageType.Name, kind, name, payload.Length);

            var m = new Message(payload);

            if (ProducerSettingsByMessageType.TryGetValue(messageType, out var producerSettings))
            {
                try
                {
                    var messageModifier = producerSettings.GetMessageModifier();
                    messageModifier(message, m);
                }
                catch (Exception e)
                {
                    _logger.LogWarning(e, "The configured message modifier failed for message type {0} and message {1}", messageType, message);
                }
            }

            if (kind == PathKind.Topic)
            {
                var topicProducer = _producerByTopic.GetOrAdd(name);
                await topicProducer.SendAsync(m).ConfigureAwait(false);
            }
            else
            {
                var queueProducer = _producerByQueue.GetOrAdd(name);
                await queueProducer.SendAsync(m).ConfigureAwait(false);
            }

            _logger.LogDebug("Delivered message {0} of type {1} on {2} {3}", message, messageType.Name, kind, name);
        }

        public override Task ProduceToTransport(Type messageType, object message, string path, byte[] payload, MessageWithHeaders messageWithHeaders = null)
        {
            // determine the SMB topic name if its a Azure SB queue or topic
            var kind = _kindMapping.GetKind(messageType, path);

            return ProduceToTransport(messageType, message, path, payload, kind);
        }

        public static readonly string RequestHeaderReplyToKind = "reply-to-kind";

        public override Task ProduceRequest(object request, MessageWithHeaders requestMessage, string path, ProducerSettings producerSettings)
        {
            if (requestMessage is null) throw new ArgumentNullException(nameof(requestMessage));

            requestMessage.SetHeader(RequestHeaderReplyToKind, (int)Settings.RequestResponse.PathKind);
            return base.ProduceRequest(request, requestMessage, path, producerSettings);
        }

        public override Task ProduceResponse(object request, MessageWithHeaders requestMessage, object response, MessageWithHeaders responseMessage, ConsumerSettings consumerSettings)
        {
            if (requestMessage is null) throw new ArgumentNullException(nameof(requestMessage));
            if (consumerSettings is null) throw new ArgumentNullException(nameof(consumerSettings));

            var replyTo = requestMessage.Headers[ReqRespMessageHeaders.ReplyTo];
            var kind = (PathKind)requestMessage.GetHeaderAsInt(RequestHeaderReplyToKind);

            var responseMessagePayload = SerializeResponse(consumerSettings.ResponseType, response, responseMessage);

            return ProduceToTransport(consumerSettings.ResponseType, response, replyTo, responseMessagePayload, kind);
        }

        #endregion
    }
}
