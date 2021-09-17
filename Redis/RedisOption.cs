using System;
using Microsoft.Extensions.Configuration;

public class RedisOption
{
	public string ConnectionString { get; set; }

	public RedisOption(IConfiguration config)
	{
		if (config == null)
		{
			throw new ArgumentNullException("config");
		}
		config.GetSection("redis").Bind(this);
	}
}
