package timber_configuration

import (
	"github.com/hashicorp/go-retryablehttp"
	"log"
	"os"
	"time"
)

type ConfigurationOption func(*Configuration)

type Configuration struct {
	Env                 string
	StaticParams        map[string]string
	Logger              *log.Logger
	ClientConfiguration HttpClientConfiguration
	QueueConfiguration  InMemoryQueueConfiguration
	CronStringSpec      string
}

type HttpClientConfiguration struct {
	BaseUrl             string
	NetDialTimeout      time.Duration
	TLSHandshakeTimeout time.Duration
	ConnectionTimeout   time.Duration
	RetryWaitMin        *time.Duration
	RetryWaitMax        *time.Duration
	RetryMax            *int
	CheckRetry          *retryablehttp.CheckRetry
	Backoff             *retryablehttp.Backoff
	Logger              *log.Logger
}

type InMemoryQueueConfiguration struct {
	MaxBufferSize                int
	MaxSecondsBeforeBatchTimeout int
	MaxEventsBatchSize           int
}

func NewConfiguration(opts ...ConfigurationOption) Configuration {
	retryAttempts := 4
	defaultLogger := log.New(os.Stderr, "", log.LstdFlags)
	defaultConfig := Configuration{
		Env:            "default",
		StaticParams:   nil,
		Logger:         defaultLogger,
		CronStringSpec: "*/5 * * * *", //every 5 minutes
		ClientConfiguration: HttpClientConfiguration{
			BaseUrl:             "http://localhost:8484",
			NetDialTimeout:      5 * time.Second,
			TLSHandshakeTimeout: 5 * time.Second,
			ConnectionTimeout:   time.Second * 10,
			Logger:              defaultLogger,
			RetryWaitMin:        nil,
			RetryWaitMax:        nil,
			RetryMax:            &retryAttempts,
			CheckRetry:          nil,
			Backoff:             nil,
		},
		QueueConfiguration: InMemoryQueueConfiguration{
			MaxBufferSize:                200000,
			MaxSecondsBeforeBatchTimeout: 3,
			MaxEventsBatchSize:           2097152, // 2MB,
		},
	}

	for _, opt := range opts {
		opt(&defaultConfig)
	}

	return defaultConfig
}

func WithCronSpec(cronSpec string) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.CronStringSpec = cronSpec
	}
}

func WithEnv(env string) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.Env = env
	}
}

func WithStaticParams(params map[string]string) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.StaticParams = params
	}
}

func WithLogger(logger *log.Logger) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.Logger = logger
		configuration.ClientConfiguration.Logger = logger //use the same logger for http retry and timber logger
	}
}

func WithBaseUrl(url string) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.ClientConfiguration.BaseUrl = url
	}
}

func WithHttpBackoffPolicy(backoff retryablehttp.Backoff) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.ClientConfiguration.Backoff = &backoff
	}
}

func WithHttpCheckRetry(check retryablehttp.CheckRetry) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.ClientConfiguration.CheckRetry = &check
	}
}

func WithCustomQueueConfiguration(config InMemoryQueueConfiguration) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.QueueConfiguration = config
	}
}

func WithCustomHttpClientConfiguration(config HttpClientConfiguration) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.ClientConfiguration = config
	}
}

func WithCustomHttpClientDialOptions(connectionTimeout time.Duration, tLSHandshakeTimeout time.Duration, netDialTimeout time.Duration) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.ClientConfiguration.ConnectionTimeout = connectionTimeout
		configuration.ClientConfiguration.TLSHandshakeTimeout = tLSHandshakeTimeout
		configuration.ClientConfiguration.NetDialTimeout = netDialTimeout
	}
}

func WithCustomHttpClientRetryOptions(waitMin time.Duration, waitMax time.Duration, maxRetries int) ConfigurationOption {
	return func(configuration *Configuration) {
		configuration.ClientConfiguration.RetryWaitMin = &waitMin
		configuration.ClientConfiguration.RetryWaitMax = &waitMax
		configuration.ClientConfiguration.RetryMax = &maxRetries
	}
}
