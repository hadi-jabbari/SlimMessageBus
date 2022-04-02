namespace SlimMessageBus.Host.Collections
{
    using SlimMessageBus.Host.Interceptor;

    public class RuntimeTypeCache
    {
        public GenericInterfaceTypeCache ProducerInterceptorType { get; }
        public GenericInterfaceTypeCache PublishInterceptorType { get; }

        public GenericInterfaceTypeCache ConsumerInterceptorType { get; }
        public GenericInterfaceTypeCache HandlerInterceptorType { get; }

        public RuntimeTypeCache()
        {
            ProducerInterceptorType = new GenericInterfaceTypeCache(typeof(IProducerInterceptor<>), nameof(IProducerInterceptor<object>.OnHandle));
            PublishInterceptorType = new GenericInterfaceTypeCache(typeof(IPublishInterceptor<>), nameof(IPublishInterceptor<object>.OnHandle));
            
            ConsumerInterceptorType = new GenericInterfaceTypeCache(typeof(IConsumerInterceptor<>), nameof(IConsumerInterceptor<object>.OnHandle));
            HandlerInterceptorType = new GenericInterfaceTypeCache(typeof(IRequestHandlerInterceptor<,>), nameof(IRequestHandlerInterceptor<object, object>.OnHandle));
        }
    }
}
