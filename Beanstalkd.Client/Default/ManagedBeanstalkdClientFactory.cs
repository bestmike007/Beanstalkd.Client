using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Proxies;

namespace Beanstalkd.Client.Default
{
    public sealed class ManagedBeanstalkdClientFactory : RealProxy
    {
        private IBeanstalkdClient _client;
        private readonly string _host;
        private readonly int _port;
        private string _currentTube = "default";
        private readonly List<string> _watchList = new List<string> { "default" };

        private ManagedBeanstalkdClientFactory(string host, int port)
            : base(typeof(IBeanstalkdClient))
        {
            _host = host;
            _port = port;
        }

        public static IBeanstalkdClient Create(string host, int port)
        {
            return (IBeanstalkdClient)new ManagedBeanstalkdClientFactory(host, port).GetTransparentProxy();
        }

        private IMessage Invoke(IMessage msg, bool retry)
        {
            try
            {
                if (_client == null || _client.Disposed)
                {
                    _client = new BeanstalkdClient(_host, _port);
                    _watchList.ForEach(tube => _client.Watch(tube));
                    if (!_watchList.Contains("default")) _client.Ignore("default");
                    if (_currentTube != "default") _client.Use(_currentTube);
                }
                var methodCall = (IMethodCallMessage)msg;
                var method = (MethodInfo)methodCall.MethodBase;
                var result = method.Invoke(_client, methodCall.InArgs);
                if (method.Name == "Use" && null != result as string) _currentTube = result as string;
                if (method.Name == "Watch" && methodCall.ArgCount == 1
                    && result is uint && !_watchList.Contains((string)methodCall.InArgs[0]))
                    _watchList.Add((string)methodCall.InArgs[0]);
                return new ReturnMessage(result, null, 0, methodCall.LogicalCallContext, methodCall);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e);
                if (!(e is TargetInvocationException) || e.InnerException == null)
                    return new ReturnMessage(e, msg as IMethodCallMessage);

                var ex = e.InnerException as BeanstalkdException;
                if (retry && ex != null && ex.Code == BeanstalkdExceptionCode.ConnectionError)
                {
                    return Invoke(msg, false);
                }
                return new ReturnMessage(e.InnerException, msg as IMethodCallMessage);
            }
        }

        public override IMessage Invoke(IMessage msg)
        {
            return Invoke(msg, true);
        }
    }
}
