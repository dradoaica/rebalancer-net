using System;

namespace Rebalancer.Core.Logging
{
    /// <summary>
    /// Temporary hack
    /// </summary>
    public class ConsoleRebalancerLogger : IRebalancerLogger
    {
        private LogLevel logLevel;

        public ConsoleRebalancerLogger()
        {
            logLevel = LogLevel.DEBUG;
        }

        public ConsoleRebalancerLogger(LogLevel logLevel)
        {
            this.logLevel = logLevel;
        }

        public void Error(string clientId, string text)
        {
            if ((int)logLevel <= 3)
            {
                Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: ERROR : {clientId} : {text}");
            }
        }

        public void Error(string clientId, Exception ex)
        {
            if ((int)logLevel <= 3)
            {
                Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: ERROR : {clientId} : {ex.ToString()}");
            }
        }

        public void Error(string clientId, string text, Exception ex)
        {
            if ((int)logLevel <= 3)
            {
                Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: ERROR : {clientId} : {text} : {ex.ToString()}");
            }
        }

        public void Warn(string clientId, string text)
        {
            if ((int)logLevel <= 2)
            {
                Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: WARN : {clientId} : {text}");
            }
        }

        public void Info(string clientId, string text)
        {
            if ((int)logLevel <= 1)
            {
                Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: INFO  : {clientId} : {text}");
            }
        }

        public void Debug(string clientId, string text)
        {
            if (logLevel == 0)
            {
                Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss,fff")}: DEBUG : {clientId} : {text}");
            }
        }

        public void SetMinimumLevel(LogLevel logLevel)
        {
            this.logLevel = logLevel;
        }
    }
}
