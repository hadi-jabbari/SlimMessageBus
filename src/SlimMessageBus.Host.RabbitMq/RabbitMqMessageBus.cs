using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using SlimMessageBus.Host.Config;
using SlimMessageBus.Host.Serialization;

namespace SlimMessageBus.Host.RabbitMq
{
    public class RabbitMqMessageBus : MessageBusBase
    {
        private readonly ILogger logger;
        public RabbitMqMessageBusSettings ProviderSettings { get; }
        public RabbitMqMessageBus(MessageBusSettings settings, RabbitMqMessageBusSettings providerSettings) : base(settings)
        {
            logger = LoggerFactory.CreateLogger<RabbitMqMessageBus>();
            ProviderSettings = providerSettings ?? throw new ArgumentNullException(nameof(providerSettings));
            
            OnBuildProvider();
        }
        
        public IMessageSerializer HeaderSerializer
            => ProviderSettings.HeaderSerializer ?? Serializer;
        
        public override Task ProduceToTransport(object message, string path, byte[] messagePayload, 
            IDictionary<string, object> messageHeaders = null,
            CancellationToken cancellationToken = default)
        {
            AssertActive();
            var messageType = message.GetType();
            var factory = new ConnectionFactory() { HostName = ProviderSettings.HostName };
            using(var connection = factory.CreateConnection())
            using(var channel = connection.CreateModel())
            {
                var basicProperties = channel.CreateBasicProperties();
                if (messageHeaders != null && messageHeaders.Count > 0)
                {
                    foreach (var keyValue in messageHeaders)
                    {
                        var valueBytes = HeaderSerializer.Serialize(typeof(object), keyValue.Value);
                        basicProperties.Headers.Add(keyValue.Key, valueBytes);
                    }
                }
                channel.ExchangeDeclare(exchange: path, type: ExchangeType.Fanout);
                channel.BasicPublish(exchange: path,
                    routingKey: "",
                    basicProperties: basicProperties,
                    body: messagePayload);
            }
            return Task.CompletedTask;
        }
    }
}