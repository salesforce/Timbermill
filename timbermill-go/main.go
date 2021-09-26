package main

import (
	"datorama.com/timbermill/timbermill"
	"errors"
)

func main() {
	timbermill.Init()

	params := timbermill.NewTimberParams()
	tra := timbermill.TimberLoggerInstance.NewTransaction()
	tra.Start("test-main", *params.Strings("test-strings", "123").Context("test-context", "context"))
	tra.LogString("test-strings2", "123")
	f1(tra)
	tra.Success(nil)
	tra.Success(nil)

	timbermill.TimberLoggerInstance.SendEvents() //TODO: to do with cron event
}

func f1(transaction *timbermill.TimberTransaction) {
	params := timbermill.NewTimberParams()
	params.MetricsFromMap(map[string]int{"m1": 1, "m2": 2}).Text("testing-test", "t1")
	transaction.Start("test-f1", *params)
	f2(transaction)
	transaction.Error(errors.New("just an error"), timbermill.NewTimberParams().Text("success", "true"))
}

func f2(transaction *timbermill.TimberTransaction) {
	params := timbermill.NewTimberParams()
	params.ContextFromMap(map[string]string{"c1": "v1", "c2": "v2"}).Metrics("testing-metrics", 1)
	transaction.Start("test-f2", *params)
	transaction.Spot("spot-event", *(timbermill.NewTimberParams().Text("spot-event-test", "test")))
	transaction.Success(nil)
}
