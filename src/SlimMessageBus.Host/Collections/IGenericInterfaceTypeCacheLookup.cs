namespace SlimMessageBus.Host.Collections
{
    using System;
    using System.Collections.Generic;

    public interface IGenericInterfaceTypeCacheLookup
    {
        GenericInterfaceTypeCache this[Type key] { get; }
    }

    public class GenericInterfaceTypeCacheLookup : IGenericInterfaceTypeCacheLookup
    {
        private readonly Dictionary<Type, GenericInterfaceTypeCache> cacheByOpenGenericType = new();

        public GenericInterfaceTypeCache this[Type key] => cacheByOpenGenericType[key];

        public void Add(Type openGenericType, string methodName)
        {
            cacheByOpenGenericType.Add(openGenericType, new GenericInterfaceTypeCache(openGenericType, methodName));
        }
    }

    public class BusGenericInterfaceTypeCacheLookup : GenericInterfaceTypeCacheLookup
    {
        public BusGenericInterfaceTypeCacheLookup()
        {
            Add(typeof(IPublisherInterceptor<>), nameof(IPublisherInterceptor<object>.OnHandle));
            Add(typeof(IConsumerInterceptor<>), nameof(IConsumerInterceptor<object>.OnHandle));

            Add(typeof(IRequestInterceptor<,>), nameof(IRequestInterceptor<object, object>.OnHandle));
            Add(typeof(IRequestHandlerInterceptor<,>), nameof(IRequestHandlerInterceptor<object, object>.OnHandle));

            Add(typeof(IConsumer<>), nameof(IConsumer<object>.OnHandle));
            Add(typeof(IRequestHandler<,>), nameof(IRequestHandler<object, object>.OnHandle));
        }
    }
}
