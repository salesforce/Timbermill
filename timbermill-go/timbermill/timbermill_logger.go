package timbermill

import (
	"datorama.com/timbermill/client"
	"datorama.com/timbermill/dto"
	"datorama.com/timbermill/timber_configuration"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"log"
)

var TimberLoggerInstance timberLogger

func Init(configs ...timber_configuration.Configuration) {
	var config timber_configuration.Configuration
	if len(configs) == 0 {
		config = timber_configuration.NewConfiguration()
	} else {
		config = configs[0]
	}
	TimberLoggerInstance = timberLogger{
		client:       client.NewTimbermillClient(config.ClientConfiguration),
		logger:       config.Logger,
		staticParams: config.StaticParams,
		env:          config.Env,
	}
	initDefaultEventQueue(config.QueueConfiguration)
	c := cron.New()
	TimberLoggerInstance.cron = c
	entryId, err := c.AddFunc(config.CronStringSpec, TimberLoggerInstance.SendEvents)
	if err != nil {
		TimberLoggerInstance.logger.Panic("failed to add cron job! exiting!")
	} else {
		TimberLoggerInstance.cronEntryId = entryId
	}
}

type timberLogger struct {
	client       client.TimbermillClient
	logger       *log.Logger
	staticParams map[string]string
	env          string
	cronEntryId  cron.EntryID
	cron         *cron.Cron
}

func (s *timberLogger) NewTransaction() *TimberTransaction {
	return emptyTransaction(s.staticParams)
}

// OnGoingTransaction - Used to transfer context between multiple services.
func (s *timberLogger) OnGoingTransaction(taskId string) *TimberTransaction {
	newTransaction := emptyTransaction(s.staticParams)
	newTransaction.SetOngoingTask(taskId)

	return newTransaction
}

func (s *timberLogger) SendEvents() {
	events := (*EventQueue).Events()
	s.client.SendEvents(&dto.TimbermillEvents{
		FieldType: "EventsWrapper",
		Id:        s.generateNewUniqId(),
		Events:    events,
	})
}

func (s *timberLogger) Stop() { //stop current cron run
	s.cron.Stop()
}

func (s *timberLogger) Remove() { //remove cron permanently
	s.cron.Remove(s.cronEntryId)
}

func (s *timberLogger) generateNewUniqId() string {
	return uuid.New().String()
}
