package core

type DeliveryMode int

const (
	Broadcast DeliveryMode = 0
	Target    DeliveryMode = 10
)

type Update struct {
	Data        any
	Mode        DeliveryMode
	Connections []string
}
