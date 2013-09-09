namespace Beanstalkd.Client
{
    public class Job
    {
        public Job(uint jobId, byte[] data)
        {
            JobId = jobId;
            Data = data;
        }

        public uint JobId { get; protected set; }
        public byte[] Data { get; protected set; }
    }
}