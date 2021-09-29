package timbermill

import (
	"datorama.com/timbermill/client"
	"datorama.com/timbermill/dto"
	"datorama.com/timbermill/timber_configuration"
	"datorama.com/timbermill/utils"
	"errors"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// Write code here to run before tests
	initForTests()
	// Run tests
	exitVal := m.Run()

	// Write code here to run after tests

	// Exit with exit value from tests
	os.Exit(exitVal)
}

func initForTests() {

	config := timber_configuration.NewConfiguration()
	TimberLoggerInstance = timberLogger{
		client:       client.NewTimbermillClient(config.ClientConfiguration),
		logger:       config.Logger,
		staticParams: config.StaticParams,
		env:          config.Env,
	}
	initDefaultEventQueue(config.QueueConfiguration)
}

func TestTimber_basicSuccessFlow(t *testing.T) {
	params := NewTimberParams().Text("basic-success-text", "text")
	transaction := TimberLoggerInstance.NewTransaction()
	name := "basic-success"
	t1, _ := transaction.Start(name, *params)
	t2, _ := transaction.Success(nil)
	events := (*EventQueue).Events()

	assert.Equal(t, len(events), 2)
	assert.Equal(t, *events[0], newTimbermillEvent(StartEvent, t1, &name, nil, *params, events[0].Time))
	assert.Equal(t, *events[1], newTimbermillEvent(SuccessEvent, t2, nil, nil, *NewTimberParams(), events[1].Time))
}

func TestTimber_basicErrorFlow(t *testing.T) {
	params := NewTimberParams().Metrics("basic-error-metrics", 1)
	transaction := TimberLoggerInstance.NewTransaction()
	name := "basic-error"
	t1, _ := transaction.Start(name, *params)
	errorParams := NewTimberParams().Text("error-text", "text")
	err := errors.New("error")
	t2, _ := transaction.Error(err, errorParams)
	errorParams.Text(Error, err.Error()).Text(StackTrace, utils.StackTraceFromError(err, 2))

	events := (*EventQueue).Events()

	assert.Equal(t, len(events), 2)
	assert.Equal(t, *events[0], newTimbermillEvent(StartEvent, t1, &name, nil, *params, events[0].Time))
	assert.Equal(t, *events[1], newTimbermillEvent(ErrorEvent, t2, nil, nil, *errorParams, events[1].Time))
}

func TestTimber_basicInfoWithDaysToKeepFlow(t *testing.T) {
	params := NewTimberParams().Text("basic-info-text", "text")
	transaction := TimberLoggerInstance.NewTransaction()
	name := "basic-info"
	dateToDeleteOption := NewTimberOptionWithDaysToKeep(4)
	t1, _ := transaction.Start(name, *params, *dateToDeleteOption)
	transaction.LogString("info-string", "s")
	t3, _ := transaction.Success(nil)
	events := (*EventQueue).Events()

	assert.Equal(t, len(events), 3)
	assert.Equal(t, *events[0], newTimbermillEvent(StartEvent, t1, &name, nil, *params, events[0].Time, dateToDeleteOption))
	assert.Equal(t, *events[1], newTimbermillEvent(InfoEvent, t1, nil, nil, *NewTimberParams().Strings("info-string", "s"), events[1].Time))
	assert.Equal(t, *events[2], newTimbermillEvent(SuccessEvent, t3, nil, nil, *NewTimberParams(), events[2].Time))
}

func TestTimber_basicSpotWithStaticParamsFlow(t *testing.T) {
	staticMap := map[string]string{"param1": "1", "param2": "2"}
	TimberLoggerInstance.staticParams = staticMap
	transaction := TimberLoggerInstance.NewTransaction()
	name := "basic-spot"
	t1, _ := transaction.Spot(name, *NewTimberParams())
	events := (*EventQueue).Events()
	event := newTimbermillEvent(SpotEvent, t1, &name, nil, *NewTimberParams().StringsFromMap(staticMap), events[0].Time)
	spotSuccess := SpotSuccess
	event.SpotStatus = &spotSuccess

	assert.Equal(t, len(events), 1)
	assert.Equal(t, *events[0], event)
	TimberLoggerInstance.staticParams = map[string]string{}
}

func TestTimber_spotCorruptedFlow(t *testing.T) {
	transaction := TimberLoggerInstance.NewTransaction()
	transaction.LogContext("c1", "context")
	transaction.Success(nil)
	events := (*EventQueue).Events()

	assert.Equal(t, len(events), 2)
	assert.Equal(t, *events[0].SpotStatus, SpotCorrupted)
	assert.Equal(t, *events[1].SpotStatus, SpotCorrupted)
}

func TestTimber_nestedFlow(t *testing.T) {
	transaction := TimberLoggerInstance.NewTransaction()
	t1, _ := transaction.Start("t1", *NewTimberParams())
	t2, _ := transaction.Start("t2", *NewTimberParams())
	transaction.LogString("s2", "s2")
	t3, _ := transaction.Start("t1", *NewTimberParams())
	transaction.LogContext("c3", "c3")
	transaction.Success(nil)
	transaction.Success(nil)
	transaction.Success(nil)
	events := (*EventQueue).Events()

	assert.Equal(t, len(events), 8)
	assert.Equal(t, *events[1].ParentId, t1)
	assert.Equal(t, events[2].TaskId, t2)
	assert.Equal(t, *events[3].ParentId, t2)
	assert.Equal(t, events[4].TaskId, t3)
	assert.Equal(t, events[7].TaskId, t1)
}

func newTimbermillEvent(fieldType, taskId string, name, parentId *string, params TimberParams, t time.Time, options ...*TimberOption) dto.TimbermillEvent {
	var dateToDelete *time.Time
	if len(options) > 0 {
		dateToDelete = &options[0].dateToDelete
	}
	return dto.TimbermillEvent{
		FieldType:    fieldType,
		Time:         t,
		TaskId:       taskId,
		Name:         name,
		ParentId:     parentId,
		Strings:      params.strings,
		Text:         params.text,
		Context:      params.context,
		Metrics:      params.metrics,
		Env:          TimberLoggerInstance.env,
		DateToDelete: dateToDelete,
		SpotStatus:   nil,
	}
}
