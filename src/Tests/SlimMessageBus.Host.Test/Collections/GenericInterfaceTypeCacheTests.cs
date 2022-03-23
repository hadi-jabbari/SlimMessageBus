namespace SlimMessageBus.Host.Test.Collections
{
    using FluentAssertions;
    using Moq;
    using SlimMessageBus.Host.Collections;
    using SlimMessageBus.Host.DependencyResolver;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Xunit;

    public class GenericInterfaceTypeCacheTests
    {
        private readonly Mock<IConsumerInterceptor<SomeMessage>> consumerInterceptorMock;
        private readonly Mock<IDependencyResolver> scopeMock;

        public GenericInterfaceTypeCacheTests()
        {
            consumerInterceptorMock = new Mock<IConsumerInterceptor<SomeMessage>>();

            scopeMock = new Mock<IDependencyResolver>();
            scopeMock.Setup(x => x.Resolve(typeof(IEnumerable<IConsumerInterceptor<SomeMessage>>))).Returns(() => new[] { consumerInterceptorMock.Object });
        }

        [Fact]
        public void ResolveAll_Works()
        {
            // arrange
            var subject = new GenericInterfaceTypeCache(typeof(IConsumerInterceptor<>), nameof(IConsumerInterceptor<object>.OnHandle));

            // act
            var interceptors = subject.ResolveAll(scopeMock.Object, typeof(SomeMessage));

            // assert
            scopeMock.Verify(x => x.Resolve(typeof(IEnumerable<IConsumerInterceptor<SomeMessage>>)), Times.Once);
            scopeMock.VerifyNoOtherCalls();
        }

        [Fact]
        public async Task When_Get_Then_ReturnsValidMethodAndInterceptorGenericType()
        {
            // arrange
            var message = new SomeMessage();
            var ct = new CancellationToken();
            Func<Task> next = () => Task.CompletedTask;
            var bus = new Mock<IMessageBus>();
            var path = "path";
            var headers = new Dictionary<string, object>();
            var consumer = new object();

            consumerInterceptorMock.Setup(x => x.OnHandle(message, ct, next, bus.Object, path, headers, consumer)).Returns(Task.CompletedTask);

            var subject = new GenericInterfaceTypeCache(typeof(IConsumerInterceptor<>), nameof(IConsumerInterceptor<object>.OnHandle));

            // act
            var interceptorType = subject.Get(typeof(SomeMessage));

            var task = (Task)interceptorType.Method.Invoke(consumerInterceptorMock.Object, new object[] { message, ct, next, bus.Object, path, headers, consumer });
            await task;

            // assert
            interceptorType.GenericType.Should().Be(typeof(IConsumerInterceptor<SomeMessage>));
            interceptorType.MessageType.Should().Be(typeof(SomeMessage));

            consumerInterceptorMock.Verify(x => x.OnHandle(message, ct, next, bus.Object, path, headers, consumer), Times.Once);
            consumerInterceptorMock.VerifyNoOtherCalls();
        }
    }
}
