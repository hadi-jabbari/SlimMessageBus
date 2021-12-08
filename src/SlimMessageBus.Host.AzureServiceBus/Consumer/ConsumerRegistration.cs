namespace SlimMessageBus.Host.AzureServiceBus.Consumer
{
    using Microsoft.Azure.ServiceBus;
    using SlimMessageBus.Host.Config;

    public class ConsumerRegistration
    {
        public AbstractConsumerSettings Settings { get; }
        public IMessageProcessor<Message> Processor { get; }

        public ConsumerRegistration(AbstractConsumerSettings settings, IMessageProcessor<Message> processor)
        {
            Settings = settings;
            Processor = processor;
        }
    }
}