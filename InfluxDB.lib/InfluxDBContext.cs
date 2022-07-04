using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using Microsoft.Extensions.Logging;

namespace InfluxDBShim.lib
{
    public class InfluxDBContext : IDisposable
    {
        private InfluxDBClient? _client;

        private readonly ILogger _logger;

        public InfluxDBContext(ILogger logger)
        {
            _logger = logger;
        }

        public bool Connect(string connectionString, string token)
        {
            _client = InfluxDBClientFactory.Create(connectionString, token);

            _logger.LogDebug("Initialized Client okay");

            return true;
        }

        public async Task<List<T>> QueryAsync<T>(string query)
        {
            var queryApi = _client?.GetQueryApi();

            if (queryApi == null)
            {
                throw new InvalidOperationException("QueryAPI was null");
            }

            return await queryApi.QueryAsync<T>(query);
        }

        public async Task<string> QueryRawStringAsync<T>(string query)
        {
            var queryApi = _client?.GetQueryApi();

            if (queryApi == null)
            {
                throw new InvalidOperationException("QueryAPI was null");
            }

            return await queryApi.QueryRawAsync(query);
        }

        public async Task<bool> WriteAsync<T>(T newObject, WritePrecision precision = WritePrecision.Ns)
        {
            var writeApi = _client?.GetWriteApiAsync();

            if (writeApi == null)
            {
                throw new InvalidOperationException("writeApi was null");
            }

            await writeApi.WriteMeasurementAsync(newObject, precision, "bucket_name", "org_id");
            
            return true;
        }

        public void Dispose()
        {
            _client?.Dispose();

            GC.SuppressFinalize(this);
        }
    }
}