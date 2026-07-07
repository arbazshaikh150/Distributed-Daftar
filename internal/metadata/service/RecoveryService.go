package service

/*

	Worker  ---> Pool the Job ( replication_db ) and then --> publish this into the queue
			---> after that the subscriber of the queue will get the notification
			---> then the subscriber ( file storage service -> other service not mine)
			---> will listen it and then do the copy operation --> after that it will call
				me ( metadata service ) with a request of success copy --> Commiting in 
				transaction on both table (metadata , replication_data)

				work is done
*/

// Using docker for rabbitmq