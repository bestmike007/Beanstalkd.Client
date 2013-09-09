using System;
using System.Collections.Generic;
using System.Text;

namespace Beanstalkd.Client
{
    public enum ReserveStatus
    {
        Timeout,
        Reserved,
        DeadlineSoon
    }
}