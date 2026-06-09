package dto

import (
	"time"
)

type TimbermillEvents struct {
	FieldType string             `json:"@type" default:"EventsWrapper"`
	Id        string             `json:"id"`
	Events    []*TimbermillEvent `json:"events"`
}

type TimbermillEvent struct {
	FieldType    string            `json:"@type"`
	Time         time.Time         `json:"time"`
	TaskId       string            `json:"taskId"`
	Name         *string           `json:"name"`
	ParentId     *string           `json:"parentId"`
	Strings      map[string]string `json:"strings"`
	Text         map[string]string `json:"text"`
	Context      map[string]string `json:"context"`
	Metrics      map[string]int    `json:"metrics"`
	Env          string            `json:"env"`
	DateToDelete *time.Time        `json:"dateToDelete"`
	SpotStatus   *string           `json:"status"`
}
