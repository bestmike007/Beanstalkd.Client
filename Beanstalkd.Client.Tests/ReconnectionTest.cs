using System;
using System.Threading;
using Beanstalkd.Client.Default;
using NUnit.Framework;

namespace Beanstalkd.Client.Tests
{
    [TestFixture]
    public class ReconnectionTest
    {
        private const string BeanstalkdHost = "localhost";
        private const string TestTube = "test";

        public ReconnectionTest()
        {
            LogProvider.Current.Register((level, msg) => Console.WriteLine("[{0}] {1}", level, msg));
            try
            {
                using (var client = new BeanstalkdClient(BeanstalkdHost))
                {
                    client.Watch(TestTube);
                    client.Ignore("default");
                    Job job;
                    while (ReserveStatus.Reserved == client.Reserve(0, out job)) client.Delete(job.JobId);
                }
            }
            catch (BeanstalkdException ex)
            {
                Console.WriteLine("Beanstalk error: {0}.", ex.Code);
            }
        }

        /// <summary>
        /// Run this test and stop/start the beanstalkd server
        /// </summary>
        [TestCase]
        public void ConnectionTest()
        {
            var client = ManagedBeanstalkdClientFactory.Create(BeanstalkdHost);
            var retry = 10;
            Assert.AreEqual("default", client.CurrentTube);
            Assert.AreEqual(TestTube, client.Use(TestTube));
            Assert.AreEqual(TestTube, client.CurrentTube);

            Assert.AreEqual(2, client.Watch(TestTube));
            Assert.True(client.Ignore("default"));
            var watchList = client.WatchList;
            Assert.AreEqual(1, watchList.Count);
            Assert.AreEqual(TestTube, watchList[0]);

            while (true)
            {
                try
                {
                    Thread.Sleep(1000);
                    Assert.AreEqual(TestTube, client.CurrentTube);
                    watchList = client.WatchList;
                    Assert.AreEqual(1, watchList.Count);
                    Assert.AreEqual(TestTube, watchList[0]);
                }
                catch (BeanstalkdException ex)
                {
                    if (--retry == 0) break;
                    Console.WriteLine("Error: {0}", ex.Code);
                }
            }
            client.Dispose();
        }

    }
}