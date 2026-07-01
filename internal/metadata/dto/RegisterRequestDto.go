package dto

type RegisterNodeRequest struct {
	Host              string	`json:"host"`
	Port              int		`json:"port"`
	TotalCapacity     int64		`json:"totalCapacity"`
}
