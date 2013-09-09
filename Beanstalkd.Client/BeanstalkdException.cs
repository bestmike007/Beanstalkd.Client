using System;
using System.Collections.Generic;
using System.Text;

namespace Beanstalkd.Client
{
    public class BeanstalkdException : Exception
    {
        public BeanstalkdExceptionCode Code { get; protected set; }

        public BeanstalkdException(BeanstalkdExceptionCode code)
        {
            Code = code;
        }
    }
}