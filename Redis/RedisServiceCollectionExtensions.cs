using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

public static class RedisServiceCollectionExtensions
{
	public static IServiceCollection AddRedisMultiplexer(this IServiceCollection services, IConfiguration config)
	{
		if (services == null)
		{
			throw new ArgumentNullException("services");
		}
		if (config == null)
		{
			throw new ArgumentNullException("config");
		}
		services.AddOptions();
		RedisOption redisOption = new RedisOption(config);
		services.Add(ServiceDescriptor.Singleton<ConnectionMultiplexer>(ConnectionMultiplexer.Connect(redisOption.ConnectionString, (TextWriter)null)));
		return services;
	}
}
