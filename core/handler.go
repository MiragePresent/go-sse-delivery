package core

type Handler interface {
	Handle(*Signal) (*Update, error)
}
