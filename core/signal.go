package core

type Signal struct {
	ConnectionID string      `json:"connectionId"`
	Type         string      `json:"type"`
	Data         interface{} `json:"data"`
}
