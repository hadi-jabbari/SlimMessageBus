namespace SlimMessageBus.Host.Integration
{
    using Microsoft.Extensions.DependencyInjection;
    using System;
    using Xunit;
    using SlimMessageBus.Host.MsDependencyInjection;
    using SlimMessageBus.Host.Serialization.Json;
    using SlimMessageBus.Host.Hybrid;
    using SlimMessageBus.Host.Memory;
    using SlimMessageBus.Host.Config;
    using SlimMessageBus.Host.AzureServiceBus;
    using System.Threading.Tasks;
    using Xunit.Abstractions;
    using SlimMessageBus.Host.Test.Common;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;
    using SecretStore;
    using System.Collections.Generic;
    using System.Diagnostics;
    using FluentAssertions;
    using System.Linq;

    public class HybridTests
    {
        private IServiceProvider serviceProvider;

        private readonly XunitLoggerFactory _loggerFactory;
        private readonly ILogger<HybridTests> _logger;
        private readonly IConfigurationRoot _configuration;

        public HybridTests(ITestOutputHelper testOutputHelper)
        {
            _loggerFactory = new XunitLoggerFactory(testOutputHelper);
            _logger = _loggerFactory.CreateLogger<HybridTests>();

            _configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

            Secrets.Load(@"..\..\..\..\..\secrets.txt");

        }

        private void SetupBus(Action<MessageBusBuilder> memoryBuilder = null, Action<MessageBusBuilder> asbBuilder = null, Action<IServiceCollection> servicesBuilder = null)
        {
            var services = new ServiceCollection();

            services.AddSingleton<ILoggerFactory>(_loggerFactory);

            services.AddSlimMessageBus((mbb, svp) =>
            {
                var settings = new HybridMessageBusSettings
                {
                    ["Memory"] = (mbb) =>
                    {
                        memoryBuilder?.Invoke(mbb);
                        mbb.WithProviderMemory(new MemoryMessageBusSettings());
                    },
                    ["AzureSB"] = (mbb) =>
                    {
                        var connectionString = Secrets.Service.PopulateSecrets(_configuration["Azure:ServiceBus"]);

                        asbBuilder?.Invoke(mbb);
                        mbb.WithProviderServiceBus(new ServiceBusMessageBusSettings(connectionString));
                    }
                };

                mbb
                    .WithSerializer(new JsonMessageSerializer())
                    .WithProviderHybrid(settings);
            });

            servicesBuilder?.Invoke(services);

            serviceProvider = services.BuildServiceProvider();
        }

        [Fact]
        public async Task When_PublishToMemoryBus_Given_InsideConsumerWithMessageScope_Then_MessageScopeIsCarriedOverToMemoryBusConsumer()
        {
            // arrange
            SetupBus(
                memoryBuilder: (mbb) =>
                {
                    mbb.Produce<InternalMessage>(x => x.DefaultTopic(x.MessageType.Name));
                    mbb.Consume<InternalMessage>(x => x.Topic(x.MessageType.Name).WithConsumer<InternalMessageConsumer>());
                },
                asbBuilder: (mbb) =>
                {
                    mbb.Produce<ExternalMessage>(x => x.DefaultTopic("test-ping"));
                    mbb.Consume<ExternalMessage>(x => x.Topic("test-ping").SubscriptionName("subscriber-0").WithConsumer<ExternalMessageConsumer>());
                },
                servicesBuilder: services =>
                {
                    services.AddTransient<InternalMessageConsumer>();
                    services.AddTransient<ExternalMessageConsumer>();

                    // Unit of work should be shared between InternalMessageConsumer and ExternalMessageConsumer
                    services.AddScoped<UnitOfWork>();

                    services.AddSingleton<SingletonStore>();
                }
            );

            var bus = serviceProvider.GetRequiredService<IPublishBus>();

            // act
            await bus.Publish(new ExternalMessage());

            // assert
            var singletonStore = serviceProvider.GetRequiredService<SingletonStore>();

            // wait until arrives
            var stopwatch = Stopwatch.StartNew();
            while (singletonStore.Count != 2 && stopwatch.Elapsed.TotalSeconds < 10)
            {
                await Task.Delay(1000);
            }

            var snapshot = singletonStore.Snapshot();

            snapshot.Count.Should().Be(2);
            var grouping = snapshot.GroupBy(x => x.Item1, x => x.Item2).ToDictionary(x => x.Key, x => x.ToList());

            grouping.Count.Should().Be(1);
            
            // in this order
            grouping.Values.First()[0].Should().Be(nameof(ExternalMessageConsumer));
            grouping.Values.First()[1].Should().Be(nameof(InternalMessageConsumer));
        }

        internal class SingletonStore
        {
            private readonly IList<(Guid, string)> list = new List<(Guid, string)>();

            public void Add(Guid correlationId, string name)
            {
                lock (list)
                {
                    list.Add((correlationId, name));
                }
            }

            public IList<(Guid, string)> Snapshot()
            {
                lock (list)
                {
                    var snapshot = new List<(Guid, string)>(list);
                    return snapshot;
                }
            }

            public int Count
            {
                get
                {
                    lock (list)
                    {
                        return list.Count;
                    }
                }
            }
        }

        internal class UnitOfWork
        {
            public Guid CorrelationId { get; } = Guid.NewGuid();

            public Task Commit() => Task.CompletedTask;
        }

        internal class ExternalMessageConsumer : IConsumer<ExternalMessage>
        {
            private readonly IMessageBus bus;
            private readonly UnitOfWork unitOfWork;
            private readonly SingletonStore singletonStore;

            public ExternalMessageConsumer(IMessageBus bus, UnitOfWork unitOfWork, SingletonStore singletonStore)
            {
                this.bus = bus;
                this.unitOfWork = unitOfWork;
                this.singletonStore = singletonStore;
            }

            public async Task OnHandle(ExternalMessage message, string path)
            {
                singletonStore.Add(unitOfWork.CorrelationId, nameof(ExternalMessageConsumer));

                // some processing

                await bus.Publish(new InternalMessage());

                // some processing

                await unitOfWork.Commit();
            }
        }

        internal class InternalMessageConsumer : IConsumer<InternalMessage>
        {
            private readonly UnitOfWork unitOfWork;
            private readonly SingletonStore singletonStore;

            public InternalMessageConsumer(UnitOfWork unitOfWork, SingletonStore singletonStore)
            {
                this.unitOfWork = unitOfWork;
                this.singletonStore = singletonStore;
            }

            public Task OnHandle(InternalMessage message, string path)
            {
                singletonStore.Add(unitOfWork.CorrelationId, nameof(InternalMessageConsumer));

                // some processing

                return Task.CompletedTask;
            }
        }

        internal class ExternalMessage
        {
        }

        internal class InternalMessage
        {
        }
    }
}
