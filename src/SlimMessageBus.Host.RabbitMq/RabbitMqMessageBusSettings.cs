using SlimMessageBus.Host.Serialization;

namespace SlimMessageBus.Host.RabbitMq
{
    public class RabbitMqMessageBusSettings
    {
        /// <summary>
        /// The rabbitmq host name.
        /// </summary>
        public string HostName { get; set; }
        /// <summary>
        /// Serializer used to serialize Kafka message header values. If not specified the default serializer will be used (setup as part of the bus config).
        /// </summary>
        public IMessageSerializer HeaderSerializer { get; set; }
    }
}