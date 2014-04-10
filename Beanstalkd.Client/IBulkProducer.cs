using System.Collections.Generic;

namespace Beanstalkd.Client
{
    public interface IBulkProducer
    {
        List<uint?> Put(string tube, List<byte[]> data, uint priority = 4294967295, uint delay = 0, uint ttr = 10);
    }
}
