package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/database"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/model"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/queue"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*

	Worker  ---> Pool the Job ( replication_db ) and then --> publish this into the queue
			---> after that the subscriber of the queue will get the notification
			---> then the subscriber ( file storage service -> other service not mine)
			---> will listen it and then do the copy operation --> after that it will call
				me ( metadata service ) with a request of success copy --> Commiting in
				transaction on both table (metadata , replication_data)

				work is done
*/

/*
	Here i can upload more than 1 time in the queue since there are many pods
	then for the consumer side
	it received some request then i will do
	1st --> locking of the job and then doing the commit

	and if something happen in between the locking
	the commit will not be occur

	flow :

	i will publish --> [m1 , m2 , m1 , m2  , m1 , m1 , m1]

	now consumer (file service / other services)
	takes m1 --> lock it ( 1st phase ) ( ttl will be x minutes )
	then copy it --> then commit request --> if ok ( lock is removed and Ack is sent and then)
	file is being store in the node

	if something error occur --> (response sent will abort message) --> file will automatically delete if it is being copied

*/

// Using docker for rabbitmq
type RepairMessage struct {
	JobId      uuid.UUID `json:"jobId"`
	FileId     uuid.UUID `json:"fileId"`
	CopyNode   uuid.UUID `json:"copyNode"`
	TargetNode uuid.UUID `json:"targetNode"`
}

func StartRecoveryWorker(ctx context.Context) {
	// Using ticker
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	/// Using goroutines and channels
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			event, err := processPendingRequest()
			if err != nil {
				continue
			}

			err = PublishRepairEvent(ctx, *event)
			if err != nil {
				MarkFailed(*event, err)
				continue
			}
			MarkPublished(*event)
		}
	}
}

/*
Think of some optimizations --> How can i optimize it
TODO : ( since i am picking the 10 events )

	there might be a case where all the pods publish that same 10 event in the queue
	then there will be redundant copy of same event and also there will be many waste network calls

	e.g. 5 pods --> 10 events
	same time they publish the events --> each 5 seconds --> (50 events is being pushed)
	out of that 40 events are useless --> when we do the locking of the event it will fail
	(40 fail network call in each 5 seconds)

	for processing 20 events -->
	assume for processing 1 event --> 10 ms
	for 10 event --> ( i have 50 total of them )
	10 * 10ms + 40 * (2ms) = 180ms
	20 msg --> 360ms

	if i use locking mechanism
	assuming i am sending 1 event at the queue --> time required (12ms)
	then for 20 event --> (5 events are parallel)
	(20 / 5) * 12ms = 48ms
*/
func processPendingRequest() (*model.OutboxEvent, error) {
	var events model.OutboxEvent
	now := time.Now()
	expiredBefore := now.Add(-1 * time.Minute)

	// Database transactions
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		err := tx.Clauses(clause.Locking{
			Strength: "UPDATE",
			Options:  "SKIP LOCKED",
		}).Where(
			"status = ? OR (status = ? AND locked_at < ?)",
			enums.PENDING,
			enums.PROGRESS,
			expiredBefore,
		).Order("created_at ASC").
			Limit(1).First(&events).Error

		if err != nil {
			return err
		}

		// now updating
		return tx.Model(&events).Updates(map[string]interface{}{
			"status":    enums.PROGRESS,
			"locked_at": now,
		}).Error
	})

	if err != nil {
		return nil, err
	}
	return &events, nil
}

// Publishing repair event
/*
	Based on number of transaction i will decide which part i have to cache it
	and which part i have to take from the database

	since there is many database operation

	i can use WAL ( write ahead logs ) feature from postgres database
	and use it for the event published info
*/

func PublishRepairEvent(ctx context.Context, event model.OutboxEvent) error {
	if queue.RabbitChannel == nil {
		return errors.New("RabbitMQ Channel is not open")
	}

	queueName := os.Getenv("REPLICA_REPAIR_QUEUE")
	if queueName == "" {
		return errors.New("REPLICA_REPAIR_QUEUE is not present")
	}

	// Finding the target node and the replica node

	// step1 : Finding the actual data from the replica db
	var replica model.ReplicaData
	err := database.DB.First(&replica, "job_id = ?", event.JobId).Error
	if err != nil {
		return err
	}

	// step2 : Converting it into the desired shape for the queue
	msg := RepairMessage{
		JobId:      replica.JobId,
		FileId:     replica.RunID,
		CopyNode:   replica.CopyNode,
		TargetNode: replica.TargetNode,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// step3: publishing it into the rabbitMQ
	return queue.RabbitChannel.PublishWithContext(
		ctx,
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

// Marking the events to be published
func MarkPublished(event model.OutboxEvent) error {
	now := time.Now()

	return database.DB.Model(&event).Updates(map[string]interface{}{
		"status":       enums.PUBLISHED,
		"processed_at": &now,
		"locked_at":    nil,
		"last_error":   "",
	}).Error
}

func MarkFailed(event model.OutboxEvent, err error) error {
	return database.DB.Model(&event).Updates(map[string]interface{}{
		"status":      enums.PENDING,
		"locked_at":   nil,
		"retry_count": gorm.Expr("retry_count + 1"),
		"last_error":  err.Error(),
	}).Error
}
