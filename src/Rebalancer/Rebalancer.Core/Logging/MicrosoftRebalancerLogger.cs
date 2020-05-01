using Microsoft.Extensions.Logging;
using System;

namespace Rebalancer.Core.Logging
{
    /// <summary>
    /// Temporary hack
    /// </summary>
    public class MicrosoftRebalancerLogger : IRebalancerLogger
    {
        private LogLevel logLevel;
        private readonly ILogger logger;

        public MicrosoftRebalancerLogger(ILogger logger)
        {
            this.logger = logger;
            logLevel = LogLevel.DEBUG;
        }

        public MicrosoftRebalancerLogger(ILogger logger, LogLevel logLevel)
        {
            this.logger = logger;
            this.logLevel = logLevel;
        }

        public void Error(string clientId, string text)
        {
            if ((int)logLevel <= 3)
            {
                logger?.LogError($"{DateTime.Now.ToString("hh:mm:ss,fff")}: ERROR : {clientId} : {text}");
            }
        }

        public void Error(string clientId, Exception ex)
        {
            if ((int)logLevel <= 3)
            {
                logger?.LogError($"{DateTime.Now.ToString("hh:mm:ss,fff")}: ERROR : {clientId} : {ex.ToString()}");
            }
        }

        public void Error(string clientId, string text, Exception ex)
        {
            if ((int)logLevel <= 3)
            {
                logger?.LogError($"{DateTime.Now.ToString("hh:mm:ss,fff")}: ERROR : {clientId} : {text} : {ex.ToString()}");
            }
        }

        public void Warn(string clientId, string text)
        {
            if ((int)logLevel <= 2)
            {
                logger?.LogWarning($"{DateTime.Now.ToString("hh:mm:ss,fff")}: WARN : {clientId} : {text}");
            }
        }

        public void Info(string clientId, string text)
        {
            if ((int)logLevel <= 1)
            {
                logger?.LogInformation($"{DateTime.Now.ToString("hh:mm:ss,fff")}: INFO  : {clientId} : {text}");
            }
        }

        public void Debug(string clientId, string text)
        {
            if (logLevel == 0)
            {
                logger?.LogDebug($"{DateTime.Now.ToString("hh:mm:ss,fff")}: DEBUG : {clientId} : {text}");
            }
        }

        public void SetMinimumLevel(LogLevel logLevel)
        {
            this.logLevel = logLevel;
        }
    }
}
