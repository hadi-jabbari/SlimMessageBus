namespace SlimMessageBus.Host.RabbitMq.Test
{
    using System;
    using Xunit;

    public class RabbitMqMessageBusTest : IDisposable
    {
        [Fact]
        public void GetMessageKey()
        {
        }
        class MessageA
        {
            public byte[] Key { get; } = Guid.NewGuid().ToByteArray();
        }
        
        class MessageB
        {
        }

        public void Dispose()
        {
        }
    }
}
