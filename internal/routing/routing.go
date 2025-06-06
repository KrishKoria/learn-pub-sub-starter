package routing

const (
	ArmyMovesPrefix = "army_moves"

	WarRecognitionsPrefix = "war"

	PauseKey = "pause"

	GameLogSlug = "game_logs"
)

const (
	ExchangePerilDirect = "peril_direct"
	ExchangePerilTopic  = "peril_topic"
	ExchangePerilDLX    = "peril_dlx"
)

const (
	QueueTypeDurable = iota
	QueueTypeTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)