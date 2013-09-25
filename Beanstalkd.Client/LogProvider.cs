using System;
using System.Collections.Generic;

namespace Beanstalkd.Client
{
    public enum LogLevel
    {
        Debug, Info, Warn, Error, Fatal
    }

    public delegate void LogHanlder(LogLevel level, string msg);

    public class LogProvider
    {
        private static LogProvider _current;
        public static LogProvider Current
        {
            get { return _current ?? (_current = new LogProvider()); }
        }
        private LogProvider() { }

        private event EventHandler OnLog;

        public void Register(LogHanlder logger)
        {
            OnLog += (sender, args) =>
            {
                var info = (KeyValuePair<LogLevel, string>)sender;
                logger(info.Key, info.Value);
            };
        }

        public void Register(LogLevel level, Action<string> logger)
        {
            OnLog += (sender, args) =>
            {
                var info = (KeyValuePair<LogLevel, string>)sender;
                if (level == info.Key) logger(info.Value);
            };
        }

        public void Log(LogLevel level, string message)
        {
            Log(level, null, message);
        }

        public void Log(LogLevel level, Exception ex, string message)
        {
            if (null == OnLog) return;
            OnLog(
                ex == null
                    ? new KeyValuePair<LogLevel, string>(level, message)
                    : new KeyValuePair<LogLevel, string>(level, string.Format("{0}\r\n{1}", message, ex)),
                null);
        }

        public void LogFormat(LogLevel level, string message, params object[] args)
        {
            Log(level, string.Format(message, args));
        }

        public void LogFormat(LogLevel level, Exception ex, string message, params object[] args)
        {
            Log(level, ex, string.Format(message, args));
        }

        public void Debug(string message)
        {
            Log(LogLevel.Debug, message);
        }

        public void Debug(Exception ex, string message)
        {
            Log(LogLevel.Debug, ex, message);
        }

        public void DebugFormat(string message, params object[] args)
        {
            LogFormat(LogLevel.Debug, message, args);
        }

        public void DebugFormat(Exception ex, string message, params object[] args)
        {
            LogFormat(LogLevel.Debug, ex, message, args);
        }

        public void Info(string message)
        {
            Log(LogLevel.Info, message);
        }

        public void Info(Exception ex, string message)
        {
            Log(LogLevel.Info, ex, message);
        }

        public void InfoFormat(string message, params object[] args)
        {
            LogFormat(LogLevel.Info, message, args);
        }

        public void InfoFormat(Exception ex, string message, params object[] args)
        {
            LogFormat(LogLevel.Info, ex, message, args);
        }

        public void Warn(string message)
        {
            Log(LogLevel.Warn, message);
        }

        public void Warn(Exception ex, string message)
        {
            Log(LogLevel.Warn, ex, message);
        }

        public void WarnFormat(string message, params object[] args)
        {
            LogFormat(LogLevel.Warn, message, args);
        }

        public void WarnFormat(Exception ex, string message, params object[] args)
        {
            LogFormat(LogLevel.Warn, ex, message, args);
        }

        public void Error(string message)
        {
            Log(LogLevel.Error, message);
        }

        public void Error(Exception ex, string message)
        {
            Log(LogLevel.Error, ex, message);
        }

        public void ErrorFormat(string message, params object[] args)
        {
            LogFormat(LogLevel.Error, message, args);
        }

        public void ErrorFormat(Exception ex, string message, params object[] args)
        {
            LogFormat(LogLevel.Error, ex, message, args);
        }

        public void Fatal(string message)
        {
            Log(LogLevel.Fatal, message);
        }

        public void Fatal(Exception ex, string message)
        {
            Log(LogLevel.Fatal, ex, message);
        }

        public void FatalFormat(string message, params object[] args)
        {
            LogFormat(LogLevel.Fatal, message, args);
        }

        public void FatalFormat(Exception ex, string message, params object[] args)
        {
            LogFormat(LogLevel.Fatal, ex, message, args);
        }
    }
}
