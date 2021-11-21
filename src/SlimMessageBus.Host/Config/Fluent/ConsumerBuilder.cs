namespace SlimMessageBus.Host.Config
{
    using System;
    using System.Threading.Tasks;

    public class ConsumerBuilder<T> : AbstractConsumerBuilder
    {
        public ConsumerBuilder(MessageBusSettings settings, Type messageType = null)
            : base(settings, messageType ?? typeof(T))
        {
        }

        public ConsumerBuilder<T> Path(string path)
        {
            ConsumerSettings.Path = path;
            return this;
        }

        public ConsumerBuilder<T> Topic(string topic) => Path(topic);

        public ConsumerBuilder<T> Path(string path, Action<ConsumerBuilder<T>> pathConfig)
        {
            if (pathConfig is null) throw new ArgumentNullException(nameof(pathConfig));

            var b = Path(path);
            pathConfig(b);
            return b;
        }

        public ConsumerBuilder<T> Topic(string topic, Action<ConsumerBuilder<T>> topicConfig) => Path(topic, topicConfig);

        /// <summary>
        /// Declares type <typeparamref name="TConsumer"/> as the consumer of messages <typeparamref name="TMessage"/>.
        /// The consumer type has to implement <see cref="IConsumer{TMessage}"/> interface.
        /// </summary>
        /// <typeparam name="TConsumer"></typeparam>
        /// <returns></returns>
        public ConsumerBuilder<T> WithConsumer<TConsumer>()
            where TConsumer : class, IConsumer<T>
        {
            ConsumerSettings.ConsumerType = typeof(TConsumer);
            ConsumerSettings.ConsumerMode = ConsumerMode.Consumer;
            ConsumerSettings.ConsumerMethod = (consumer, message, name) => ((TConsumer)consumer).OnHandle((T)message, name);

            return this;
        }

        /// <summary>
        /// Declares type <typeparamref name="TConsumer"/> as the consumer of messages <typeparamref name="TMessage"/>.
        /// </summary>
        /// <typeparam name="TConsumer"></typeparam>
        /// <param name="method">Specifies how to delegate messages to the consumer type.</param>
        /// <returns></returns>
        public ConsumerBuilder<T> WithConsumer<TConsumer>(Func<TConsumer, T, string, Task> consumerMethod)
            where TConsumer : class
        {
            if (consumerMethod == null) throw new ArgumentNullException(nameof(consumerMethod));

            ConsumerSettings.ConsumerType = typeof(TConsumer);
            ConsumerSettings.ConsumerMode = ConsumerMode.Consumer;
            ConsumerSettings.ConsumerMethod = (consumer, message, name) => consumerMethod((TConsumer)consumer, (T)message, name);

            return this;
        }

        /// <summary>
        /// Declares type <typeparamref name="TConsumer"/> as the consumer of messages <typeparamref name="TMessage"/>.
        /// The consumer type has to have a method: <see cref="Task"/> <paramref name="consumerMethodName"/>(<typeparamref name="TMessage"/>, <see cref="string"/>).
        /// </summary>
        /// <typeparam name="TConsumer"></typeparam>
        /// <param name="consumerMethodName"></param>
        /// <returns></returns>
        public ConsumerBuilder<T> WithConsumer<TConsumer>(string consumerMethodName)
            where TConsumer : class
        {
            if (consumerMethodName == null) throw new ArgumentNullException(nameof(consumerMethodName));

            return WithConsumer(typeof(TConsumer), consumerMethodName);
        }

        /// <summary>
        /// Declares type <typeparamref name="TConsumer"/> as the consumer of messages <typeparamref name="TMessage"/>.
        /// The consumer type has to have a method: <see cref="Task"/> <paramref name="methodName"/>(<typeparamref name="TMessage"/>, <see cref="string"/>).
        /// </summary>
        /// <param name="consumerType"></param>
        /// <param name="methodName">If null, will default to <see cref="IConsumer{TMessage}.OnHandle(TMessage, string)"/> </param>
        /// <returns></returns>
        public ConsumerBuilder<T> WithConsumer(Type consumerType, string methodName = null)
        {
            if (methodName == null)
            {
                methodName = nameof(IConsumer<object>.OnHandle);
            }

            /// See <see cref="IConsumer{TMessage}.OnHandle(TMessage, string)"/> and <see cref="IRequestHandler{TRequest, TResponse}.OnHandle(TRequest, string)"/> 
            var numArgs = 2;
            // try to see if two param method exists
            var consumerOnHandleMethod = consumerType.GetMethod(methodName, new[] { ConsumerSettings.MessageType, typeof(string) });
            if (consumerOnHandleMethod == null)
            {
                // try to see if one param method exists
                numArgs = 1;
                consumerOnHandleMethod = consumerType.GetMethod(methodName, new[] { ConsumerSettings.MessageType });

                Assert.IsNotNull(consumerOnHandleMethod,
                    () => new ConfigurationMessageBusException($"Consumer type {consumerType} validation error: the method {methodName} with parameters of type {ConsumerSettings.MessageType} and {typeof(string)} was not found."));
            }

            // ensure the method returns a Task or Task<T>
            Assert.IsTrue(typeof(Task).IsAssignableFrom(consumerOnHandleMethod.ReturnType),
                () => new ConfigurationMessageBusException($"Consumer type {consumerType} validation error: the response type of method {methodName} must return {typeof(Task)}"));

            ConsumerSettings.ConsumerType = consumerType;
            ConsumerSettings.ConsumerMode = ConsumerMode.Consumer;
            if (numArgs == 2)
            {
                ConsumerSettings.ConsumerMethod = (consumer, message, path) => (Task)consumerOnHandleMethod.Invoke(consumer, new[] { message, path });
            }
            else
            {
                ConsumerSettings.ConsumerMethod = (consumer, message, name) => (Task)consumerOnHandleMethod.Invoke(consumer, new[] { message });
            }

            return this;
        }

        /// <summary>
        /// Number of concurrent competing consumer instances that the bus is asking for the DI plugin.
        /// This dictates how many concurrent messages can be processed at a time.
        /// </summary>
        /// <param name="numberOfInstances"></param>
        /// <returns></returns>
        public ConsumerBuilder<T> Instances(int numberOfInstances)
        {
            ConsumerSettings.Instances = numberOfInstances;
            return this;
        }

        /// <summary>
        /// Adds custom hooks for the consumer.
        /// </summary>
        /// <param name="eventsConfig"></param>
        /// <returns></returns>
        public ConsumerBuilder<T> AttachEvents(Action<IConsumerEvents> eventsConfig)
            => AttachEvents<ConsumerBuilder<T>>(eventsConfig);

        /// <summary>
        /// Enable (or disable) creation of DI child scope for each meesage.
        /// </summary>
        /// <param name="enabled"></param>
        /// <returns></returns>
        public ConsumerBuilder<T> PerMessageScopeEnabled(bool enabled)
        {
            ConsumerSettings.IsMessageScopeEnabled = enabled;
            return this;
        }

        /// <summary>
        /// Enable (or disable) disposal of consumer after message consumption.
        /// </summary>
        /// <remarks>This should be used in conjuction with <see cref="PerMessageScopeEnabled"/>. With per message scope enabled, the DI should dispose the consumer upon disposal of message scope.</remarks>
        /// <param name="enabled"></param>
        /// <returns></returns>
        public ConsumerBuilder<T> DisposeConsumerEnabled(bool enabled)
        {
            ConsumerSettings.IsDisposeConsumerEnabled = enabled;
            return this;
        }

        public ConsumerBuilder<T> Do(Action<ConsumerBuilder<T>> action) => base.Do(action);
    }
}