package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type RequestGetDocuments struct {
	List map[string]DocumentDAO
}

type ResponseDoc struct {
	List   map[string]DocumentDAO
	status string
}

type ResponseSaveDoc struct {
	Name string
}

type RequestSaveDoc struct {
	Name string
	File []byte
}

type RequestDeleteDoc struct {
	Id string
}

type ResponseDeleteDoc struct {
	Name string
}

func main() {
	go QueueGetDocuments()
	go QueueSaveDocuments()
	go QueueDeleteDocuments()

	forever := make(chan bool)
	<-forever
}

func QueueGetDocuments() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc_Storage_getDocuments", // name
		true,                       // durable
		false,                      // delete when unused
		false,                      // exclusive
		false,                      // no-wait
		nil,                        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			d.Ack(true)
			fmt.Println("processing petition: ", d.CorrelationId)

			body := ResponseDoc{GetDocuments(), "OK"}
			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(ToGOBResponseDoc(body)),
				})
			failOnError(err, "Failed to publish a message")

		}
	}()

	forever := make(chan bool)
	<-forever
}

func QueueSaveDocuments() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc_Storage_save", // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			//log.Printf("Received a message: %s", d.Body)
			d.Ack(true)
			req := FromGOBRequestSaveDoc(d.Body)

			fmt.Println("procesando save document: ", d.CorrelationId)

			savedName := UploadDocument(req.File, req.Name)
			var res ResponseSaveDoc
			if savedName != "" {
				res = ResponseSaveDoc{Name: savedName}
			} else {
				res = ResponseSaveDoc{Name: "ERROR"}
			}

			//respuesta
			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(ToGOBResponseSaveDoc(res)),
				})
			failOnError(err, "Failed to publish a message")

		}

	}()
	forever := make(chan bool)
	<-forever
}

func QueueDeleteDocuments() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc_Storage_delete", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range msgs {
			//log.Printf("Received a message: %s", d.Body)
			d.Ack(true)
			req := FromGOBRequestDeleteDoc(d.Body)

			fmt.Println("processing POST saveDoc: ", d.CorrelationId)
			//fmt.Println("el id es ", req.Id)

			savedName := DeleteDocuments(req.Id)
			var res ResponseDeleteDoc
			if savedName != "" {
				res = ResponseDeleteDoc{Name: savedName}
			} else {
				res = ResponseDeleteDoc{Name: "ERROR"}
			}

			//respuesta
			err = ch.Publish(
				"",        // exchange
				d.ReplyTo, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(ToGOBResponseDeleteDoc(res)),
				})
			failOnError(err, "Failed to publish a message")

		}

	}()
	forever := make(chan bool)
	<-forever
}

func ToGOBResponseDoc(m ResponseDoc) []byte {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(m)
	if err != nil {
		fmt.Println(`failed gob Encode`, err)
	}
	return b.Bytes()
}

func ToGOBResponseSaveDoc(m ResponseSaveDoc) []byte {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(m)
	if err != nil {
		fmt.Println(`failed gob Encode`, err)
	}
	return b.Bytes()
}

func FromGOB(by []byte) RequestGetDocuments {

	m := RequestGetDocuments{}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	err := d.Decode(&m)
	if err != nil {
		fmt.Println(`failed gob Decode`, err)
	}
	fmt.Println("saliendo del from", m)
	return m
}

func FromGOBRequestSaveDoc(by []byte) RequestSaveDoc {
	m := RequestSaveDoc{}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	err := d.Decode(&m)
	if err != nil {
		fmt.Println(`failed gob Decode`, err)
	}
	return m
}

func FromGOBRequestDeleteDoc(by []byte) RequestDeleteDoc {
	m := RequestDeleteDoc{}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	err := d.Decode(&m)
	if err != nil {
		fmt.Println(`failed gob Decode`, err)
	}
	return m
}

func ToGOBResponseDeleteDoc(m ResponseDeleteDoc) []byte {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(m)
	if err != nil {
		fmt.Println(`failed gob Encode`, err)
	}
	return b.Bytes()
}
