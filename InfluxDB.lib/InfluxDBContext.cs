using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;

using Microsoft.Extensions.Logging;

namespace InfluxDBShim.lib
{
    public class InfluxDBContext : IDisposable
    {
        private InfluxDBClient? _client;

        private readonly ILogger? _logger;

        public InfluxDBContext(ILogger? logger = null)
        {
            _logger = logger;
        }

        public bool Connect(string connectionString, string token)
        {
            _client = InfluxDBClientFactory.Create(connectionString, token);

            _logger?.LogDebug("Initialized Client okay");

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

        public async Task<List<T>> QueryAsync<T>(string measurementName, DateTime startTime, DateTime endTime)
        {
            var queryApi = _client?.GetQueryApi();

            if (queryApi == null)
            {
                throw new InvalidOperationException("QueryAPI was null");
            }

            return await queryApi.QueryAsync<T>($"SELECT * FROM {measurementName} WHERE time > {startTime} AND time < {endTime}");
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

        public async Task<bool> WriteAsync<T>(T newObject, string bucketName, string orgId, WritePrecision precision = WritePrecision.Ns)
        {
            var writeApi = _client?.GetWriteApiAsync();

            if (writeApi == null)
            {
                throw new InvalidOperationException("writeApi was null");
            }

            await writeApi.WriteMeasurementAsync(newObject, precision, bucketName, orgId);
            
            return true;
        }

        public async Task<string> CreateTokenAsync(string organization, string bucketName)
        {
            if (_client == null)
            {
                throw new InvalidOperationException("Client was null");
            }

            var orgId = (await _client.GetOrganizationsApi().FindOrganizationsAsync(org: organization)).First().Id;

            var retention = new BucketRetentionRules(BucketRetentionRules.TypeEnum.Expire, 3600);

            var bucket = await _client.GetBucketsApi().CreateBucketAsync(bucketName, retention, orgId);

            var resource = new PermissionResource(PermissionResource.TypeBuckets, bucket.Id, null, orgId);

            var read = new Permission(Permission.ActionEnum.Read, resource);

            var write = new Permission(Permission.ActionEnum.Write, resource);

            var authorization = await _client.GetAuthorizationsApi()
                .CreateAuthorizationAsync(orgId, new List<Permission> { read, write });

            return authorization.Token;
        }

        public void Dispose()
        {
            _client?.Dispose();

            GC.SuppressFinalize(this);
        }
    }
}