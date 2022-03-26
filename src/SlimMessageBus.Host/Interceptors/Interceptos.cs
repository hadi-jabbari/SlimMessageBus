namespace SlimMessageBus.Host
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IProducerInterceptor
    {
    }

    public interface IPublisherInterceptor<in TMessage>
    {
        Task OnHandle(TMessage message, CancellationToken cancellationToken, Func<Task> next, IMessageBus bus, string path, IDictionary<string, object> headers);
    }

    public interface IRequestInterceptor<in TRequest, TResponse>
    {
        Task<TResponse> OnHandle(TRequest request, CancellationToken cancellationToken, Func<Task<TResponse>> next, IMessageBus bus, string path, IDictionary<string, object> headers);
    }

    public interface IConsumerInterceptor<in TMessage>
    {
        Task OnHandle(TMessage message, CancellationToken cancellationToken, Func<Task> next, IMessageBus bus, string path, IReadOnlyDictionary<string, object> headers, object consumer);
    }

    public interface IRequestHandlerInterceptor<in TRequest, TResponse>
    {
        // ToDo: to result add headers and path
        Task<TResponse> OnHandle(TRequest request, CancellationToken cancellationToken, Func<Task<TResponse>> next, IMessageBus bus, string path, IReadOnlyDictionary<string, object> headers, object handler);
    }
}
