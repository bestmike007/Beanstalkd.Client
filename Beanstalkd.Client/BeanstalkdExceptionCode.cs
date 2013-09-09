namespace Beanstalkd.Client
{
    public enum BeanstalkdExceptionCode
    {
        OutOfMemory,
        InternalError,
        Draining,
        BadFormat,
        UnknownCommand,
        JobTooBig,
        ConnectionError,
        UnexpectedResponse
    }
}