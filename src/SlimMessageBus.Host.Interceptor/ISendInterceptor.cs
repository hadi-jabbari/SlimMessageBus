namespace SlimMessageBus.Host.Interceptor
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public interface ISendInterceptor<in TRequest, TResponse>
    {
        Task<TResponse> OnHandle(TRequest request, CancellationToken cancellationToken, Func<Task<TResponse>> next, IMessageBus bus, string path, IDictionary<string, object> headers);
    }
}
