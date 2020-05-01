using Microsoft.Data.SqlClient;
using System;
using System.Threading.Tasks;

namespace Rebalancer.SqlServer.Connections
{
    public static class ConnectionHelper
    {
        public static async Task<SqlConnection> GetOpenConnectionAsync(string connectionString)
        {
            int tries = 0;
            while (tries <= 3)
            {
                tries++;
                try
                {
                    SqlConnection connection = new SqlConnection(connectionString);
                    await connection.OpenAsync();
                    return connection;
                }
                catch (Exception ex) when (TransientErrorDetector.IsTransient(ex))
                {
                    if (tries == 3)
                    {
                        throw;
                    }

                    // wait 1, 2, 4 seconds -- would be nice to not to delay cancellation here
                    await Task.Delay(TimeSpan.FromSeconds(tries * 2));
                }
            }

            return null;
        }
    }
}
