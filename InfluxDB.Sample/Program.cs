using NLog;

var logger = LogManager.GetCurrentClassLogger();

var influxDbClient = new InfluxDBShim.lib.InfluxDBContext(null);