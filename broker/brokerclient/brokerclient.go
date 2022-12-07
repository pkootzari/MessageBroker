package brokerclient

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"
)

type Client struct {
	Host           string
	Port           string
	ConnectionType string
}

type RequestType int

const (
	SUBSCRIBE_TO_QUEUE RequestType = iota
	CONNECT_TO_TWO_WAY_QUEUE
	SEND_MESSAGE_TO_ONE_WAY_QUEUE
	SEND_MESSAGE_TO_TWO_WAY_QUEUE
)

type SendMessageRequest struct {
	TypeOfRequest RequestType
	Message       []byte
	QueueName     string
	ClientData    Client
}

const (
	BROKER_HOST = "localhost"
	BROKER_PORT = "9999"
	BROKER_TYPE = "tcp"
)

type Publisher interface {
	SendSyncMessage(message []byte, queue_name string) ([]byte, error)
	SendAsyncMessage(message []byte, queue_name string) ([]byte, chan string, error)
}

type SocketPublisher struct {
	broker_host string
	broker_port string
	broker_type string
}

func InitPublisher() Publisher {
	return &SocketPublisher{
		broker_host: BROKER_HOST,
		broker_port: BROKER_PORT,
		broker_type: BROKER_TYPE,
	}
}

func encode_send_message_request(message []byte, queue_name string) []byte {
	request := SendMessageRequest{TypeOfRequest: SEND_MESSAGE_TO_ONE_WAY_QUEUE, Message: message, QueueName: queue_name}
	var encoded_message bytes.Buffer
	encoder := gob.NewEncoder(&encoded_message)
	err := encoder.Encode(request)
	if err != nil {
		fmt.Println("error in encoding message: ", err)
	}
	return encoded_message.Bytes()
}

func (publisher *SocketPublisher) SendSyncMessage(message []byte, queue_name string) ([]byte, error) {
	connection, err := net.Dial(publisher.broker_type, publisher.broker_host+":"+publisher.broker_port)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	_, err = connection.Write(encode_send_message_request(message, queue_name))
	if err != nil {
		fmt.Println("failed to send the message", err)
		return nil, err
	}
	fmt.Printf("Sent: %s to queue: %v\n", string(message[:]), queue_name)
	buffer := make([]byte, 1024)
	mLen, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return nil, err
	}
	return buffer[:mLen], nil
}

func (publisher *SocketPublisher) SendAsyncMessage(message []byte, queue_name string) ([]byte, chan string, error) {
	connection, err := net.Dial(publisher.broker_type, publisher.broker_host+":"+publisher.broker_port)
	if err != nil {
		return nil, nil, err
	}
	_, err = connection.Write(encode_send_message_request(message, queue_name))
	if err != nil {
		fmt.Println("failed to send the message", err)
		return nil, nil, err
	}
	fmt.Printf("Sent: %s to queue: %v\n", string(message[:]), queue_name)
	reply_channel := make(chan string)
	go recieve_reply(connection, reply_channel)
	return []byte("Message recieved by the broker!"), reply_channel, nil
}

func recieve_reply(connection net.Conn, reply_channel chan string) {
	defer connection.Close()
	buffer := make([]byte, 1024)
	mLen, err := connection.Read(buffer)
	if err != nil {
		reply_channel <- string("Error reading: " + err.Error())
		return
	}
	reply_channel <- string(buffer[:mLen])
}

type Consumer interface {
	ReceiveFromBrokerForEver(handler func(net.Conn))
	SubscribeToQueue(queue_name string) ([]byte, error)
	SubscribeToTwoWayQueue(queue_name string) ([]byte, error)
	SendToTwoWayQueue(queue_name string, message []byte) ([]byte, error)
}

type SockerConsumer struct {
	reciever_host string
	reciever_port string
	reciever_type string

	broker_host string
	broker_port string
	broker_type string
}

func InitConsumer(reciever_type string, reciever_host string, reciever_port string) Consumer {
	new_client := SockerConsumer{
		reciever_host: reciever_host,
		reciever_port: reciever_port,
		reciever_type: reciever_type,
		broker_host:   BROKER_HOST,
		broker_port:   BROKER_PORT,
		broker_type:   BROKER_TYPE,
	}
	return &new_client
}

func (consumer *SockerConsumer) ReceiveFromBrokerForEver(handler func(net.Conn)) {
	server, err := net.Listen(consumer.reciever_type, consumer.reciever_host+":"+consumer.reciever_port)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer server.Close()
	fmt.Println("Listening on " + consumer.reciever_host + ":" + consumer.reciever_port)
	fmt.Println("Waiting for message from the broker...")
	for {
		connection, err := server.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go handler(connection)
	}
}

func encode_subscribe_to_queue_message(client_data Client, queue_name string) []byte {
	request := SendMessageRequest{TypeOfRequest: SUBSCRIBE_TO_QUEUE, QueueName: queue_name, ClientData: client_data}
	var encoded_message bytes.Buffer
	encoder := gob.NewEncoder(&encoded_message)
	err := encoder.Encode(request)
	if err != nil {
		fmt.Println("error in encoding message: ", err)
	}
	return encoded_message.Bytes()
}

func (consumer *SockerConsumer) SubscribeToQueue(queue_name string) ([]byte, error) {
	connection, err := net.Dial(consumer.broker_type, consumer.broker_host+":"+consumer.broker_port)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	client_data := Client{Host: consumer.reciever_host, Port: consumer.reciever_port, ConnectionType: consumer.reciever_type}
	_, err = connection.Write(encode_subscribe_to_queue_message(client_data, queue_name))
	if err != nil {
		fmt.Println("failed to send the message", err)
		return nil, err
	}
	fmt.Printf("Sent subscribe to queue request for: %s\n", queue_name)
	buffer := make([]byte, 1024)
	mLen, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return nil, err
	}
	return buffer[:mLen], nil
}

func encode_connet_to_two_way_queue_message(client_data Client, queue_name string) []byte {
	request := SendMessageRequest{TypeOfRequest: CONNECT_TO_TWO_WAY_QUEUE, QueueName: queue_name, ClientData: client_data}
	var encoded_message bytes.Buffer
	encoder := gob.NewEncoder(&encoded_message)
	err := encoder.Encode(request)
	if err != nil {
		fmt.Println("error in encoding message: ", err)
	}
	return encoded_message.Bytes()
}

func (consumer *SockerConsumer) SubscribeToTwoWayQueue(queue_name string) ([]byte, error) {
	connection, err := net.Dial(consumer.broker_type, consumer.broker_host+":"+consumer.broker_port)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	client_data := Client{Host: consumer.reciever_host, Port: consumer.reciever_port, ConnectionType: consumer.reciever_type}
	_, err = connection.Write(encode_connet_to_two_way_queue_message(client_data, queue_name))
	if err != nil {
		fmt.Println("failed to send the message", err)
		return nil, err
	}
	fmt.Printf("Sent subscribe to two way queue request for: %s\n", queue_name)
	buffer := make([]byte, 1024)
	mLen, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return nil, err
	}
	return buffer[:mLen], nil
}

func encode_send_to_two_way_queue_message(client_data Client, queue_name string, message []byte) []byte {
	request := SendMessageRequest{TypeOfRequest: SEND_MESSAGE_TO_TWO_WAY_QUEUE, QueueName: queue_name, ClientData: client_data, Message: message}
	var encoded_message bytes.Buffer
	encoder := gob.NewEncoder(&encoded_message)
	err := encoder.Encode(request)
	if err != nil {
		fmt.Println("error in encoding message: ", err)
	}
	return encoded_message.Bytes()
}

func (consumer *SockerConsumer) SendToTwoWayQueue(queue_name string, message []byte) ([]byte, error) {
	connection, err := net.Dial(consumer.broker_type, consumer.broker_host+":"+consumer.broker_port)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	client_data := Client{Host: consumer.reciever_host, Port: consumer.reciever_port, ConnectionType: consumer.reciever_type}
	_, err = connection.Write(encode_send_to_two_way_queue_message(client_data, queue_name, message))
	if err != nil {
		fmt.Println("failed to send the message", err)
		return nil, err
	}
	fmt.Printf("Sent: %s to two way queue: %v\n", string(message[:]), queue_name)
	buffer := make([]byte, 1024)
	mLen, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		return nil, err
	}
	return buffer[:mLen], nil
}
