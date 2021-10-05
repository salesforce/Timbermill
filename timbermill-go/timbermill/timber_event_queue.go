package timbermill

import (
	"datorama.com/timbermill/dto"
	"datorama.com/timbermill/timber_configuration"
	"datorama.com/timbermill/utils"
	"errors"
	"github.com/gammazero/deque"
	"strings"
	"sync"
	"time"
)

const maxCharsAllowedForNonAnalyzedFields = 8000
const maxCharsAllowedForAnalyzedFields = 900000

type FieldType int

const (
	Text FieldType = iota
	String
	Context
)

var EventQueue *TimberEventQueue

func initDefaultEventQueue(config timber_configuration.InMemoryQueueConfiguration) {
	EventQueue = newInMemoryTimberEventQueue(config)
}

type TimberEventQueue interface {
	EnqueueEvent(event *dto.TimbermillEvent) error
	Events() []*dto.TimbermillEvent
}

func newInMemoryTimberEventQueue(config timber_configuration.InMemoryQueueConfiguration) *TimberEventQueue {
	var eventQueue TimberEventQueue
	eventQueue = &inMemoryTimberEventQueue{
		maxBufferSize:                config.MaxBufferSize,
		maxSecondsBeforeBatchTimeout: config.MaxSecondsBeforeBatchTimeout,
		maxEventsBatchSize:           config.MaxEventsBatchSize,
		currentSize:                  0,
	}
	return &eventQueue
}

type inMemoryTimberEventQueue struct {
	deque                        deque.Deque
	maxBufferSize                int
	maxSecondsBeforeBatchTimeout int
	maxEventsBatchSize           int
	currentSize                  int
	sync.Mutex
}

func (eq *inMemoryTimberEventQueue) EnqueueEvent(event *dto.TimbermillEvent) error {
	eq.Lock()
	defer eq.Unlock()

	currentEventSize, err := utils.GetRealSizeOf(event)
	if err != nil {
		return err
	}
	if currentEventSize+eq.currentSize > eq.maxBufferSize {
		TimberLoggerInstance.logger.Print("max buffer size was reached!")
		return errors.New("max buffer size was accessed")
	}

	eq.deque.PushBack(event)
	eq.currentSize += currentEventSize
	return nil
}

func (eq *inMemoryTimberEventQueue) Events() []*dto.TimbermillEvent {
	eq.Lock()
	defer eq.Unlock()

	if eq.noEvents() {
		return nil
	}

	var events []*dto.TimbermillEvent
	processedEventsSize := 0
	now := time.Now()

	for processedEventsSize < eq.maxEventsBatchSize && !eq.isExceededMaxTimeToWait(now) {
		event, size := eq.dequeEvent()
		processedEventsSize += size
		if event != nil {
			events = append(events, event)
		}
	}

	return events
}

func (eq *inMemoryTimberEventQueue) dequeEvent() (*dto.TimbermillEvent, int) {
	if eq.noEvents() {
		time.Sleep(100 * time.Millisecond)
		return nil, 0
	}
	event := eq.deque.PopFront().(*dto.TimbermillEvent)
	eventSize, _ := utils.GetRealSizeOf(event)
	eq.currentSize -= eventSize
	event = eq.processEvent(event)

	return event, eventSize
}

func (eq *inMemoryTimberEventQueue) noEvents() bool {
	return eq.deque.Len() == 0
}

func (eq *inMemoryTimberEventQueue) processEvent(event *dto.TimbermillEvent) *dto.TimbermillEvent {
	event.Text = eq.processEventStringTypeEntry(event.Text, Text)
	event.Strings = eq.processEventStringTypeEntry(event.Strings, String)
	event.Context = eq.processEventStringTypeEntry(event.Context, Context)
	event.Metrics = eq.processEventIntegerTypeEntry(event.Metrics)

	return event
}

func (eq *inMemoryTimberEventQueue) processEventStringTypeEntry(entry map[string]string, fieldType FieldType) map[string]string {
	res := map[string]string{}
	for key, elem := range entry {
		newKey := strings.Replace(key, ".", "_", -1)
		newVal := ""
		if fieldType == Text {
			newVal = utils.TruncateString(elem, maxCharsAllowedForAnalyzedFields)
		} else {
			newVal = utils.TruncateString(elem, maxCharsAllowedForNonAnalyzedFields)
		}
		res[newKey] = newVal
	}

	return res
}

func (eq *inMemoryTimberEventQueue) processEventIntegerTypeEntry(entry map[string]int) map[string]int {
	res := map[string]int{}
	for key, elem := range entry {
		newKey := strings.Replace(key, ".", "_", -1)
		res[newKey] = elem
	}

	return res
}

func (eq *inMemoryTimberEventQueue) isExceededMaxTimeToWait(startTime time.Time) bool {
	return time.Now().UnixMilli()-startTime.UnixMilli() > int64(eq.maxSecondsBeforeBatchTimeout)*1000
}
