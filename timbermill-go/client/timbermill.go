package client

import (
	"bytes"
	"datorama.com/timbermill/dto"
	"datorama.com/timbermill/timber_configuration"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"io/ioutil"
	"log"
	"net"
	"net/http"
)

type TimbermillClient interface {
	SendEvents(events *dto.TimbermillEvents)
}

func NewTimbermillClient(config timber_configuration.HttpClientConfiguration) TimbermillClient {
	var netTransport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: config.NetDialTimeout,
		}).DialContext,
		TLSHandshakeTimeout: config.TLSHandshakeTimeout,
	}
	var netClient = &http.Client{
		Timeout:   config.ConnectionTimeout,
		Transport: netTransport,
	}

	retryClient := retryablehttp.NewClient()
	retryClient.HTTPClient = netClient
	retryClient.Logger = config.Logger
	setOptionalConfigurations(retryClient, &config)

	return httpRetryableTimbermillClient{netClient: retryClient, baseUrl: config.BaseUrl}
}

func setOptionalConfigurations(client *retryablehttp.Client, config *timber_configuration.HttpClientConfiguration) {
	if config.RetryMax != nil {
		client.RetryMax = *config.RetryMax
	}
	if config.RetryWaitMax != nil {
		client.RetryWaitMax = *config.RetryWaitMax
	}
	if config.RetryWaitMin != nil {
		client.RetryWaitMin = *config.RetryWaitMin
	}
	if config.Backoff != nil {
		client.Backoff = *config.Backoff
	}
	if config.CheckRetry != nil {
		client.CheckRetry = *config.CheckRetry
	}
}

type httpRetryableTimbermillClient struct {
	netClient *retryablehttp.Client
	baseUrl   string
}

func (tc httpRetryableTimbermillClient) SendEvents(events *dto.TimbermillEvents) {
	body, _ := json.Marshal(events)
	path := fmt.Sprintf("%s/events", tc.baseUrl)
	resp, err := tc.netClient.Post(path, "application/json", bytes.NewBuffer(body))
	//Handle Error
	if err != nil {
		log.Printf("An Error Occured While sending http request%v\n", err)
	}
	defer resp.Body.Close()
	//Read the response body
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("An Error Occured while reading a body %v\n", err)
	}
}
