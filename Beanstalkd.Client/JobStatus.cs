using System;
using System.Collections.Generic;
using System.Text;

namespace Beanstalkd.Client
{
    public enum JobStatus
    {
        Ready,
        Reserved,
        Buried,
        Delayed
    }
}