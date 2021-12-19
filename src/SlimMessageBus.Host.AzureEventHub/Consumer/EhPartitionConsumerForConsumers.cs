namespace SlimMessageBus.Host.AzureEventHub
{
    using Microsoft.Azure.EventHubs;
    using SlimMessageBus.Host.Config;

    /// <summary>
    /// <see cref="EhPartitionConsumer"/> implementation meant for processing messages coming to consumers (<see cref="IConsumer{TMessage}"/>) in pub-sub or handlers (<see cref="IRequestHandler{TRequest,TResponse}"/>) in request-response flows.
    /// </summary>
    public class EhPartitionConsumerForConsumers : EhPartitionConsumer
    {
        public EhPartitionConsumerForConsumers(EventHubMessageBus messageBus, ConsumerSettings consumerSettings)
            : base(messageBus, consumerSettings, new ConsumerInstanceMessageProcessor<EventData>(consumerSettings, messageBus, GetMessageWithHeaders))
        {
        }
    }
}