namespace SlimMessageBus.Host.MsDependencyInjection
{
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;
    using SlimMessageBus.Host.Config;
    using SlimMessageBus.Host.Interceptor;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Registers SlimMessageBus (<see cref="IMessageBus">) singleton instance and configures the MsDependencyInjection as the DI plugin.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="configure"></param>
        /// <param name="loggerFactory">Use a custom logger factory. If not provided it will be obtained from the DI.</param>
        /// <param name="configureDependencyResolver">Confgure the DI plugin on the <see cref="MessageBusBuilder"/>. Default is true.</param>
        /// <param name="addConsumersFromAssembly">Specifies the list of assemblies to be searched for <see cref="IConsumer{TMessage}"/> or <see cref="IRequestHandler{TRequest, TResponse}"/> implementationss. The found types are added to the DI as Transient service.</param>
        /// <param name="addConfiguratorsFromAssembly">Specifies the list of assemblies to be searched for <see cref="IMessageBusConfigurator"/>. The found types are added to the DI as Transient service.</param>
        /// <param name="addInterceptorsFromAssembly">Specifies the list of assemblies to be searched for interceptors (<see cref="IPublishInterceptor{TMessage}"/>, <see cref="ISendInterceptor{TRequest, TResponse}"/>, <see cref="IConsumerInterceptor{TMessage}"/>, <see cref="IRequestHandler{TRequest, TResponse}"/>). The found types are added to the DI as Transient service.</param>
        /// <returns></returns>
        public static IServiceCollection AddSlimMessageBus(
            this IServiceCollection services,
            Action<MessageBusBuilder, IServiceProvider> configure,
            ILoggerFactory loggerFactory = null,
            bool configureDependencyResolver = true,
            Assembly[] addConsumersFromAssembly = null,
            Assembly[] addConfiguratorsFromAssembly = null,
            Assembly[] addInterceptorsFromAssembly = null)
        {
            if (addConsumersFromAssembly != null)
            {
                services.AddMessageBusConsumersFromAssembly(filterPredicate: null, addConsumersFromAssembly);
            }

            if (addConfiguratorsFromAssembly != null)
            {
                services.AddMessageBusConfiguratorsFromAssembly(addConfiguratorsFromAssembly);
            }

            if (addInterceptorsFromAssembly != null)
            {
                services.AddMessageBusInterceptorsFromAssembly(addInterceptorsFromAssembly);
            }

            // Single master bus that holds the defined consumers and message processing pipelines
            services.AddSingleton((svp) =>
            {
                var configurators = svp.GetServices<IMessageBusConfigurator>();

                var mbb = MessageBusBuilder.Create();
                if (configureDependencyResolver)
                {
                    mbb.WithDependencyResolver(new MsDependencyInjectionDependencyResolver(svp, loggerFactory));
                }

                foreach (var configurator in configurators)
                {
                    configurator.Configure(mbb, "default");
                }

                configure(mbb, svp);

                return (IMasterMessageBus)mbb.Build();
            });

            services.AddTransient<IConsumerControl>(svp => svp.GetRequiredService<IMasterMessageBus>());

            // Register transient message bus - this is a lightweight proxy that just introduces the current DI scope
            services.AddTransient(svp => new MessageBusProxy(svp.GetRequiredService<IMasterMessageBus>(), new MsDependencyInjectionDependencyResolver(svp)));

            services.AddTransient<IMessageBus>(svp => svp.GetRequiredService<MessageBusProxy>());
            services.AddTransient<IPublishBus>(svp => svp.GetRequiredService<MessageBusProxy>());
            services.AddTransient<IRequestResponseBus>(svp => svp.GetRequiredService<MessageBusProxy>());

            return services;
        }

        private static IEnumerable<(Type Type, Type InterfaceType)> GetAllProspectTypesWithInterface(IEnumerable<Assembly> assemblies)
        {
            var foundTypes = assemblies
                .SelectMany(x => x.GetTypes())
                .Where(t => t.IsClass && !t.IsAbstract && t.IsVisible)
                .SelectMany(t => t.GetInterfaces(), (t, i) => (Type: t, InterfaceType: i));

            return foundTypes;
        }

        /// <summary>
        /// Scans the specified assemblies (using reflection) for types that implement either <see cref="IConsumer{TMessage}"/> or <see cref="IRequestHandler{TRequest, TResponse}"/>. 
        /// The found types are registered in the DI as Transient service.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="filterPredicate">Filtering predicate that allows to further narrow down the </param>
        /// <param name="assemblies">Assemblies to be scanned</param>
        /// <returns></returns>
        public static IServiceCollection AddMessageBusConsumersFromAssembly(this IServiceCollection services, Func<Type, bool> filterPredicate, params Assembly[] assemblies)
        {
            var foundTypes = GetAllProspectTypesWithInterface(assemblies)
                .Where(x => x.InterfaceType.IsGenericType && GenericTypesConsumers.Contains(x.InterfaceType.GetGenericTypeDefinition()))
                .Where(x => filterPredicate == null || filterPredicate(x.Type))
                .Select(x =>
                {
                    var genericArguments = x.InterfaceType.GetGenericArguments();
                    return new
                    {
                        ConsumerType = x.Type,
                        MessageType = genericArguments[0],
                        ResponseType = genericArguments.Length > 1 ? genericArguments[1] : null
                    };
                })
                .ToList();

            foreach (var foundType in foundTypes)
            {
                services.AddTransient(foundType.ConsumerType);
            }

            return services;
        }

        private static readonly Type[] GenericTypesConsumers = new[]
        {
            typeof(IConsumer<>),
            typeof(IRequestHandler<,>)
        };

        /// <summary>
        /// Scans the specified assemblies (using reflection) for types that implement either <see cref="IConsumer{TMessage}"/> or <see cref="IRequestHandler{TRequest, TResponse}"/>. 
        /// The found types are registered in the DI as Transient service.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="assemblies">Assemblies to be scanned</param>
        /// <returns></returns>
        public static IServiceCollection AddMessageBusConsumersFromAssembly(this IServiceCollection services, params Assembly[] assemblies)
            => services.AddMessageBusConsumersFromAssembly(filterPredicate: null, assemblies);

        /// <summary>
        /// Scans the specified assemblies (using reflection) for types that implement <see cref="IMessageBusConfigurator{TMessage}"/> and adds them to DI.
        /// This types will be use during message bus configuration.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="assemblies">Assemblies to be scanned</param>
        /// <returns></returns>
        public static IServiceCollection AddMessageBusConfiguratorsFromAssembly(this IServiceCollection services, params Assembly[] assemblies)
        {
            var foundTypes = GetAllProspectTypesWithInterface(assemblies)
                .Where(x => x.InterfaceType == typeof(IMessageBusConfigurator))
                .Select(x => x.Type)
                .ToList();

            foreach (var foundType in foundTypes)
            {
                services.AddTransient(typeof(IMessageBusConfigurator), foundType);
            }

            return services;
        }

        /// <summary>
        /// Scans the specified assemblies (using reflection) for types that implement <see cref="IPublishInterceptor{TMessage}"/> or <see cref="IConsumerInterceptor{TMessage}"/> and adds them to DI.
        /// This types will be use during message bus configuration.
        /// </summary>
        /// <param name="services"></param>
        /// <param name="assemblies">Assemblies to be scanned</param>
        /// <returns></returns>
        public static IServiceCollection AddMessageBusInterceptorsFromAssembly(this IServiceCollection services, params Assembly[] assemblies)
        {
            var foundTypes = GetAllProspectTypesWithInterface(assemblies)
                .Where(x => x.InterfaceType.IsGenericType && GenericTypesInterceptors.Contains(x.InterfaceType.GetGenericTypeDefinition()))
                .ToList();

            foreach (var foundType in foundTypes)
            {
                services.AddTransient(foundType.InterfaceType, foundType.Type);
            }

            return services;
        }

        private static readonly Type[] GenericTypesInterceptors = new[]
        {
            typeof(IProducerInterceptor<>),
            typeof(IConsumerInterceptor<>),

            typeof(IPublishInterceptor<>),

            typeof(ISendInterceptor<,>),
            typeof(IRequestHandlerInterceptor<,>)
        };
    }
}
