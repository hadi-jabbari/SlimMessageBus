namespace SlimMessageBus.Host
{
    using SlimMessageBus.Host.DependencyResolver;
    using System;

    /// <summary>
    /// A decorator for <see cref="IDependencyResolver"/> that first checks within the current DI scope (if one does exist).
    /// </summary>
    public class MessageScopeAwareDependencyResolverDecorator : IDependencyResolver
    {
        private readonly IDependencyResolver _target;

        public MessageScopeAwareDependencyResolverDecorator(IDependencyResolver target) => _target = target;

        public IChildDependencyResolver CreateScope() => _target.CreateScope();

        public object Resolve(Type type)
        {
            // try to use an message scope is present
            var existingScope = MessageScope.Current;
            if (existingScope != null)
            {
                return existingScope.Resolve(type);
            }
            return _target.Resolve(type);
        }
    }
}
