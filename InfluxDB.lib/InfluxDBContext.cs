using InfluxDB.Client;

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

        public async Task<List<InfluxDB.Client.Core.Flux.Domain.FluxRecord>> QueryAsync<FluxRecord>(string query)
        {
            var queryApi = _client?.GetQueryApi();

            if (queryApi == null)
            {
                throw new InvalidOperationException("QueryAPI was null");
            }

            var tables = await queryApi.QueryAsync(query);

            var records = new List<InfluxDB.Client.Core.Flux.Domain.FluxRecord>();

            tables.ForEach(table =>
            {
                table.Records.ForEach(record =>
                {
                    records.Add(record);                    
                });
            });

            return records;
        }

        public void Dispose()
        {
            _client?.Dispose();

            GC.SuppressFinalize(this);
        }
    }
}