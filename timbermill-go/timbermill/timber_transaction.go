package timbermill

import (
	"datorama.com/timbermill/dto"
	"datorama.com/timbermill/utils"
	"errors"
	"fmt"
	"github.com/emirpasic/gods/stacks"
	"github.com/emirpasic/gods/stacks/arraystack"
	"github.com/google/uuid"
	"strings"
	"time"
)

const (
	SpotSuccess   = "SUCCESS"
	SpotCorrupted = "CORRUPTED"
)

const (
	Error             = "exception"
	StackTrace        = "stackTrace"
	LogWithoutContext = "LogWithoutContext"
)

type TimberTransaction struct {
	ongoingTransactionStack stacks.Stack
	staticParams            map[string]string
}

func emptyTransaction(params map[string]string) *TimberTransaction {
	return &TimberTransaction{
		ongoingTransactionStack: arraystack.New(),
		staticParams:            params,
	}
}

func (t *TimberTransaction) Start(name string, params TimberParams, opts ...TimberOption) (string, error) {
	currentTaskId, err := t.ongoingTransaction()
	params.addStaticParams(t.staticParams)
	if err != nil {
		return "", err
	}
	newId := newTimberId(name)
	t.SetOngoingTask(newId)
	var dateToDelete time.Time
	if len(opts) > 0 {
		dateToDelete = opts[0].dateToDelete
	}
	return newId, (*EventQueue).EnqueueEvent(newStartEvent(newId, &name, currentTaskId, &params, dateToDelete))
}

func (t *TimberTransaction) LogParams(params *TimberParams) (string, error) {
	return t.info(params)
}

func (t *TimberTransaction) LogString(key string, val string) (string, error) {
	return t.info(NewTimberParams().Strings(key, val))
}

func (t *TimberTransaction) LogText(key string, val string) (string, error) {
	return t.info(NewTimberParams().Text(key, val))
}

func (t *TimberTransaction) LogContext(key string, val string) (string, error) {
	return t.info(NewTimberParams().Context(key, val))
}

func (t *TimberTransaction) LogMetrics(key string, val int) (string, error) {
	return t.info(NewTimberParams().Metrics(key, val))
}

func (t *TimberTransaction) Spot(name string, params TimberParams, opts ...TimberOption) (string, error) {
	parentId, err := t.ongoingTransaction()
	if err != nil {
		return "", err
	}

	params.addStaticParams(t.staticParams)
	newId := newTimberId(name)
	var dateToDelete time.Time
	if len(opts) > 0 {
		dateToDelete = opts[0].dateToDelete
	}
	return newId, (*EventQueue).EnqueueEvent(newSpotSuccessEvent(newId, &name, parentId, &params, dateToDelete))
}

func (t *TimberTransaction) Success(params *TimberParams) (string, error) {
	currentId, err := t.finishCurrent()
	if err != nil {
		return "", err
	} else if currentId == nil {
		return t.spotCorruptedEvent(params)
	}

	return *currentId, (*EventQueue).EnqueueEvent(newSuccessEvent(*currentId, params))
}

func (t *TimberTransaction) Error(clientErr error, params *TimberParams) (string, error) {
	currentId, err := t.finishCurrent()
	if err != nil {
		return "", err
	} else if currentId == nil {
		return t.spotCorruptedEvent(params)
	}

	if clientErr != nil {
		if params == nil {
			params = NewTimberParams()
		}
		params.Text(Error, clientErr.Error())
		params.Text(StackTrace, utils.StackTraceFromError(clientErr, 2)) //skip the first 2 traces because it is more general trace stack info
	}

	return *currentId, (*EventQueue).EnqueueEvent(newErrorEvent(*currentId, params))
}

func (t *TimberTransaction) info(params *TimberParams) (string, error) {
	taskId, err := t.ongoingTransaction()

	if err != nil {
		return "", err
	} else if taskId == nil {
		return t.spotCorruptedEvent(params)
	}

	return *taskId, (*EventQueue).EnqueueEvent(newInfoEvent(*taskId, params))
}

func (t *TimberTransaction) spotCorruptedEvent(params *TimberParams) (string, error) {
	if params == nil {
		params = NewTimberParams()
	}

	params.addStaticParams(t.staticParams)
	params.Text(StackTrace, utils.StackTrace())
	newId := newTimberId(LogWithoutContext)
	return newId, (*EventQueue).EnqueueEvent(newSpotCorruptedEvent(newId, LogWithoutContext, params))
}

func (t *TimberTransaction) SetOngoingTask(taskId string) {
	t.ongoingTransactionStack.Push(&taskId)
}

func (t *TimberTransaction) ongoingTransaction() (*string, error) {
	if t.ongoingTransactionStack.Empty() {
		return nil, nil
	} else {
		if taskId, ok := t.ongoingTransactionStack.Peek(); ok {
			return taskId.(*string), nil
		} else {
			TimberLoggerInstance.logger.Print("empty ongoing transaction")
			return nil, errors.New("ongoing transaction is missing")
		}
	}
}

func (t *TimberTransaction) finishCurrent() (*string, error) {
	if t.ongoingTransactionStack.Empty() {
		return nil, nil //add problematic event
	} else {
		if taskId, ok := t.ongoingTransactionStack.Pop(); ok {
			return taskId.(*string), nil
		} else {
			TimberLoggerInstance.logger.Print("failed to close transaction")
			return nil, errors.New("failed to close transaction")
		}
	}
}

func newTimberId(eventName string) string {
	uuidStr := strings.Replace(uuid.New().String(), "-", "_", -1)
	id := fmt.Sprintf("%s___%s", eventName, uuidStr)
	return id
}

func newStartEvent(id string, name *string, parentId *string, params *TimberParams, dateToDelete time.Time) *dto.TimbermillEvent {
	event := newEvent(id, name, parentId, params)
	event.FieldType = "StartEvent"
	if !dateToDelete.IsZero() {
		event.DateToDelete = &dateToDelete
	}

	return event
}

func newSpotSuccessEvent(id string, name *string, parentId *string, params *TimberParams, dateToDelete time.Time) *dto.TimbermillEvent {
	event := newEvent(id, name, parentId, params)
	event.FieldType = "SpotEvent"
	success := SpotSuccess
	event.SpotStatus = &success
	if !dateToDelete.IsZero() {
		event.DateToDelete = &dateToDelete
	}

	return event
}

func newSpotCorruptedEvent(id string, name string, params *TimberParams) *dto.TimbermillEvent {
	event := newEvent(id, &name, nil, params)
	event.FieldType = "SpotEvent"
	corrupted := SpotCorrupted
	event.SpotStatus = &corrupted

	return event
}

func newSuccessEvent(id string, params *TimberParams) *dto.TimbermillEvent {
	event := newEvent(id, nil, nil, params)
	event.FieldType = "SuccessEvent"

	return event
}

func newErrorEvent(id string, params *TimberParams) *dto.TimbermillEvent {
	event := newEvent(id, nil, nil, params)
	event.FieldType = "ErrorEvent"

	return event
}

func newInfoEvent(id string, params *TimberParams) *dto.TimbermillEvent {
	event := newEvent(id, nil, nil, params)
	event.FieldType = "InfoEvent"

	return event
}

func newEvent(id string, name *string, parentId *string, params *TimberParams) *dto.TimbermillEvent {
	if params == nil {
		params = NewTimberParams()
	}
	return &dto.TimbermillEvent{
		Time:         time.Now(),
		TaskId:       id,
		Name:         name,
		ParentId:     parentId,
		Strings:      params.strings,
		Text:         params.text,
		Context:      params.context,
		Metrics:      params.metrics,
		Env:          TimberLoggerInstance.env,
		DateToDelete: nil,
	}
}
