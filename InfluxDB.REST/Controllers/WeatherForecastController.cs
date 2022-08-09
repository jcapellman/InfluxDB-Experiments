using Microsoft.AspNetCore.Mvc;

namespace InfluxDB.REST.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private readonly ILogger<WeatherForecastController> _logger;

        private InfluxDBShim.lib.InfluxDBContext _infuxo;

        public WeatherForecastController(ILogger<WeatherForecastController> logger, InfluxDBShim.lib.InfluxDBContext infux)
        {
            _logger = logger;

            _infuxo = infux;
        }

        [HttpGet(Name = "GetWeatherForecast")]
        public async Task<IEnumerable<WeatherForecast>> Get() => await _infuxo.QueryAsync<WeatherForecast>("SELECT");
    }
}