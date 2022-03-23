namespace SlimMessageBus.Host.Collections
{
    using SlimMessageBus.Host.DependencyResolver;
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    public class GenericInterfaceTypeCache
    {
        private readonly Type openGenericType;
        private readonly string methodName;
        private readonly SafeDictionaryWrapper<Type, GenericInterfaceType> messageTypeToGenericInterfaceType;

        public GenericInterfaceTypeCache(Type openGenericType, string methodName)
        {
            this.openGenericType = openGenericType;
            this.methodName = methodName;
            messageTypeToGenericInterfaceType = new SafeDictionaryWrapper<Type, GenericInterfaceType>(CreateInterceptorType);
        }

        private GenericInterfaceType CreateInterceptorType(Type messageType)
        {
            var genericType = openGenericType.MakeGenericType(messageType);
            var method = genericType.GetMethod(methodName);
            return new GenericInterfaceType(messageType, genericType, method);
        }

        public IEnumerable<object> ResolveAll(IDependencyResolver scope, Type messageType)
        {
            var git = Get(messageType);

            // ToDo: Cache if this typically returns any instances
            var interceptors = (IEnumerable<object>)scope.Resolve(git.EnumerableOfGenericType);

            return interceptors;
        }

        public GenericInterfaceType Get(Type messageType) => messageTypeToGenericInterfaceType.GetOrAdd(messageType);

        public class GenericInterfaceType
        {
            public Type MessageType { get; }
            public Type GenericType { get; }
            public Type EnumerableOfGenericType { get; }
            public MethodInfo Method { get; }

            public GenericInterfaceType(Type messageType, Type genericType, MethodInfo method)
            {
                MessageType = messageType;
                GenericType = genericType;
                EnumerableOfGenericType = typeof(IEnumerable<>).MakeGenericType(genericType);
                Method = method;
            }
        }
    }
}
