package enums

// using the const keywords
type StatusDetail string

const (
	Active          StatusDetail = "Active"
	INACTIVE        StatusDetail = "InActive"
	COMMITTED       StatusDetail = "Committed"
	PENDING         StatusDetail = "Pending"
	PROGRESS        StatusDetail = "Progress"
	UNDERREPLICATED StatusDetail = "UnderReplicated"
	PUBLISHED       StatusDetail = "Published"
	COMPLETED       StatusDetail = "Completed"
	FAILED          StatusDetail = "Failed"
)
