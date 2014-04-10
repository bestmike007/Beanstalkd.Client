using System;
using System.Collections.Generic;
using System.Text;
using Beanstalkd.Client.Default;
using NUnit.Framework;

namespace Beanstalkd.Client.Tests
{
    [TestFixture]
    public class JobBulkProducerTest
    {
        private const string BeanstalkdHost = "192.168.1.254";
        private const string TestTube = "test";

        public JobBulkProducerTest()
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
        public void BulkPutTest()
        {
            var producer = new JobBulkProducer(BeanstalkdHost);
            var jobs = new List<byte[]>();
            for (var i = 0; i < 10000; i++)
            {
                jobs.Add(Encoding.ASCII.GetBytes(string.Format("{0}", i)));
            }
            var startTime = DateTime.Now;
            var result = producer.Put(TestTube, jobs);
            Console.WriteLine("Inserted {0} jobs using {1}ms.", jobs.Count, (DateTime.Now - startTime).TotalMilliseconds);
            Assert.True(result.TrueForAll(x => x.HasValue));
        }
    }
}