package enums

// using the const keywords
type StatusDetail string
const (
	Active StatusDetail = "Active"
	INACTIVE StatusDetail = "InActive"
	COMMITTED StatusDetail = "Committed"
	PROGRESS StatusDetail = "Progress"
	UNDERREPLICATED StatusDetail = "UnderReplicated"
)

