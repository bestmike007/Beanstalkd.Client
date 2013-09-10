using System.Threading;
using Beanstalkd.Client.Default;
using NUnit.Framework;

namespace Beanstalkd.Client.Tests
{
    [TestFixture]
    public class DefaultClientTest
    {
        private const string BeanstalkdHost = "127.0.0.1";
        private const string TestTube = "test";

        static DefaultClientTest()
        {
            using (var client = new BeanstalkdClient(BeanstalkdHost))
            {
                client.Watch(TestTube);
                client.Ignore("default");
                Job job;
                while (ReserveStatus.Reserved == client.Reserve(0, out job)) client.Delete(job.JobId);
            }
        }

        [TestCase]
        public void ConnectionTest()
        {
            var client = new BeanstalkdClient(BeanstalkdHost);
            Assert.AreEqual("default", client.CurrentTube);
            Assert.AreEqual(TestTube, client.Use(TestTube));
            Assert.AreEqual(TestTube, client.CurrentTube);
            Assert.AreEqual(2, client.Watch(TestTube));
            Assert.True(client.Ignore("default"));
            var watchList = client.WatchList;
            Assert.AreEqual(1, watchList.Count);
            Assert.AreEqual(TestTube, watchList[0]);
            Assert.Less(1, client.ListTubes().Count);

            Assert.Less(0, client.Stats().ConnectionCount);
            client.Dispose();
            Assert.Throws<BeanstalkdException>(() => Assert.AreEqual(TestTube, client.CurrentTube));
        }

        [TestCase]
        public void ProducerConcumerTest()
        {
            using (var client = new BeanstalkdClient(BeanstalkdHost))
            {
                Assert.AreEqual(TestTube, client.Use(TestTube));
                Assert.AreEqual(TestTube, client.CurrentTube);
                Assert.AreEqual(2, client.Watch(TestTube));
                Assert.True(client.Ignore("default"));

                Job job;
                Assert.AreEqual(ReserveStatus.Timeout, client.Reserve(0, out job));
                Assert.Less(0, client.Put(new byte[] { 97 }));
                Assert.AreEqual(1, client.StatsTube(TestTube).CurrentJobsReady);
                Assert.AreEqual(ReserveStatus.Reserved, client.Reserve(0, out job));
                Assert.AreEqual(97, job.Data[0]);
                Assert.AreEqual(1, client.StatsTube(TestTube).CurrentJobsReserved);
                Assert.AreEqual(JobStatus.Reserved, client.StatsJob(job.JobId).Status);
                Assert.True(client.Delete(job.JobId));

                Assert.AreEqual(0, client.StatsTube(TestTube).CurrentJobsReserved);
                Assert.AreEqual(0, client.StatsTube(TestTube).CurrentJobsReady);

                Assert.Less(0, client.Stats().ConnectionCount);
            }
        }

        [TestCase]
        public void DelayTest()
        {
            using (var client = new BeanstalkdClient(BeanstalkdHost))
            {
                Assert.AreEqual(TestTube, client.Use(TestTube));
                Assert.AreEqual(TestTube, client.CurrentTube);
                Assert.AreEqual(2, client.Watch(TestTube));
                Assert.True(client.Ignore("default"));

                Assert.Less(0, client.Put(new byte[] { 97 }, delay: 1));
                Assert.AreEqual(0, client.StatsTube(TestTube).CurrentJobsReady);

                Job job;
                Assert.AreEqual(ReserveStatus.Timeout, client.Reserve(0, out job));
                Thread.Sleep(1500);
                Assert.AreEqual(ReserveStatus.Reserved, client.Reserve(0, out job));
                Assert.AreEqual(97, job.Data[0]);
                Assert.AreEqual(JobStatus.Reserved, client.StatsJob(job.JobId).Status);
                Assert.True(client.Delete(job.JobId));
            }
        }

        [TestCase]
        public void BuryTest()
        {
            using (var client = new BeanstalkdClient(BeanstalkdHost))
            {
                Assert.AreEqual(TestTube, client.Use(TestTube));
                Assert.AreEqual(TestTube, client.CurrentTube);
                Assert.AreEqual(2, client.Watch(TestTube));
                Assert.True(client.Ignore("default"));

                Assert.Less(0, client.Put(new byte[] { 97 }));
                Assert.AreEqual(1, client.StatsTube(TestTube).CurrentJobsReady);

                Job job;
                Assert.AreEqual(ReserveStatus.Reserved, client.Reserve(0, out job));
                Assert.True(client.Bury(job.JobId));

                Assert.AreEqual(JobStatus.Buried, client.StatsJob(job.JobId).Status);
                Assert.AreEqual(1, client.Kick(1));

                Assert.AreEqual(JobStatus.Ready, client.StatsJob(job.JobId).Status);

                Assert.AreEqual(ReserveStatus.Reserved, client.Reserve(0, out job));
                Assert.True(client.Delete(job.JobId));
            }
        }

        [TestCase]
        public void PeekTest()
        {
            using (var client = new BeanstalkdClient(BeanstalkdHost))
            {
                Assert.AreEqual(TestTube, client.Use(TestTube));
                Assert.AreEqual(TestTube, client.CurrentTube);
                Assert.AreEqual(2, client.Watch(TestTube));
                Assert.True(client.Ignore("default"));

                Assert.Less(0, client.Put(new byte[] { 97 }));
                Assert.AreEqual(1, client.StatsTube(TestTube).CurrentJobsReady);

                var job = client.PeekReady();
                Assert.NotNull(job);
                var jobId = job.JobId;
                Assert.AreEqual(ReserveStatus.Reserved, client.Reserve(out job));
                Assert.AreEqual(jobId, job.JobId);
                Assert.Null(client.PeekReady());
                Assert.True(client.Release(jobId, delay: 1));
                Assert.NotNull(client.PeekDelayed());
                Assert.AreEqual(jobId, client.PeekDelayed().JobId);

                Thread.Sleep(1500);

                Assert.AreEqual(ReserveStatus.Reserved, client.Reserve(out job));
                Assert.True(client.Bury(jobId));
                Assert.NotNull(client.PeekBuried());
                Assert.AreEqual(jobId, client.PeekBuried().JobId);

                Assert.AreEqual(1, client.Kick(1));
                Assert.NotNull(client.Peek(jobId));
                Assert.AreEqual(ReserveStatus.Reserved, client.Reserve(out job));
                Assert.True(client.Delete(job.JobId));
            }
        }

        [TestCase]
        public void TouchTest()
        {
            using (var client = new BeanstalkdClient(BeanstalkdHost))
            {
                Assert.AreEqual(TestTube, client.Use(TestTube));
                Assert.AreEqual(TestTube, client.CurrentTube);
                Assert.AreEqual(2, client.Watch(TestTube));
                Assert.True(client.Ignore("default"));

                Job job;
                Assert.Less(0, client.Put(new byte[] { 97 }, ttr: 3));
                Assert.AreEqual(ReserveStatus.Reserved, client.Reserve(out job));
                var jobId = job.JobId;
                Thread.Sleep(2500);
                var timeLeft = client.StatsJob(jobId).TimeLeft;
                Assert.Greater(2, timeLeft);
                Assert.True(client.Touch(jobId));
                Assert.Less(timeLeft, client.StatsJob(jobId).TimeLeft);
                Assert.True(client.Delete(jobId));
            }
        }
    }
}