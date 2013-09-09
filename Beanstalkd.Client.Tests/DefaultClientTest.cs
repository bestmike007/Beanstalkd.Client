using System;
using Beanstalkd.Client.Default;
using NUnit.Framework;

namespace Beanstalkd.Client.Tests
{
    [TestFixture]
    public class DefaultClientTest
    {
        [TestCase]
        public void ConnectionTest()
        {
            try
            {
                var client = new BeanstalkdClient("192.168.1.254");
                Assert.AreEqual("default", client.CurrentTube);
                Assert.AreEqual("test", client.Use("test"));
                Assert.AreEqual("test", client.CurrentTube);
                Assert.AreEqual(2, client.Watch("test"));
                Assert.True(client.Ignore("default"));
                var watchList = client.WatchList;
                Assert.AreEqual(1, watchList.Count);
                Assert.AreEqual("test", watchList[0]);
                Assert.Less(1, client.ListTubes().Count);

                Job job;
                Assert.AreEqual(ReserveStatus.Timeout, client.Reserve(0, out job));
                Assert.Less(0, client.Put(new byte[] {97}));
                Assert.AreEqual(1, client.StatsTube("test").CurrentJobsReady);
                Assert.AreEqual(ReserveStatus.Reserved, client.Reserve(0, out job));
                Assert.AreEqual(97, job.Data[0]);
                Assert.AreEqual(1, client.StatsTube("test").CurrentJobsReserved);
                Assert.AreEqual(JobStatus.Reserved, client.StatsJob(job.JobId).Status);
                Assert.True(client.Delete(job.JobId));

                Assert.AreEqual(0, client.StatsTube("test").CurrentJobsReserved);
                Assert.AreEqual(0, client.StatsTube("test").CurrentJobsReady);

                Assert.Less(0, client.Stats().ConnectionCount);
                client.Dispose();
                Assert.Throws<BeanstalkdException>(() => Assert.AreEqual("test", client.CurrentTube));
            }
            catch (BeanstalkdException ex)
            {
                Console.WriteLine(ex.Code.ToString());
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
        }
    }
}