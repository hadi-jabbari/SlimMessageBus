namespace SlimMessageBus.Host.Interceptor
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IPublishInterceptor<in TMessage>
    {
        Task OnHandle(TMessage message, CancellationToken cancellationToken, Func<Task> next, IMessageBus bus, string path, IDictionary<string, object> headers);
    }
}
