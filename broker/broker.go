package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/broker/brokerclient"
)

type Client brokerclient.Client

var clients = []Client{}

type Message struct {
	recievers  []*Client
	message    string
	message_Id int
}

type Queue struct {
	name      string
	max_len   int
	messages  []Message
	recievers []*Client
}

type TwoWayQueue struct {
	name          string
	one_way_queus [2]Queue
}

var queues = []Queue{}
var two_way_queues = []TwoWayQueue{}

type MessageState int

const MESSAGE_STATE_SIZE int = 2048

var messages_state [MESSAGE_STATE_SIZE]int

// if messages_state[i] is equal to zero it means message with id i is acknowleged
// if it is equal to j it means j acknowlegement must come in order to insure all
// its recievers got the message

func init_queues() {
	queues = append(
		queues,
		Queue{
			name:      "owqueue",
			max_len:   100,
			messages:  make([]Message, 0, 100),
			recievers: []*Client{},
		},
	)

	queues = append(
		queues,
		Queue{
			name:      "zerocapacityqueue",
			max_len:   0,
			messages:  make([]Message, 0),
			recievers: []*Client{},
		},
	)

	two_way_queues = append(
		two_way_queues,
		TwoWayQueue{
			name: "twqueue",
			one_way_queus: [2]Queue{
				{max_len: 100, messages: make([]Message, 0, 100), recievers: []*Client{}},
				{max_len: 100, messages: make([]Message, 0, 100), recievers: []*Client{}},
			},
		},
	)
}

func add_message_to_queue(q *Queue, message []byte, message_Id int) error {
	if len(q.messages) < q.max_len {
		q.messages = append(
			q.messages,
			Message{recievers: q.recievers, message: string(message), message_Id: message_Id},
		)
		return nil
	} else {
		return errors.New("maximum capacity of queue reached")
	}
}

func send_messages_in_queues() {
	for i := 0; i < len(queues); i++ {
		go send_queue_messages_till_its_empty(&queues[i])
	}
	for i := 0; i < len(two_way_queues); i++ {
		go send_queue_messages_till_its_empty(&two_way_queues[i].one_way_queus[0])
		go send_queue_messages_till_its_empty(&two_way_queues[i].one_way_queus[1])
	}
}

func send_queue_messages_till_its_empty(queue *Queue) {
	for {
		message := get_message_from_queue(queue)
		if message != nil {
			send_message(*message)
		} else {
			time.Sleep(1 * time.Second)
		}
	}

}

func get_message_from_queue(queue *Queue) *Message {
	if len(queue.messages) > 0 {
		message := queue.messages[0]
		queue.messages = queue.messages[1:]
		return &message
	}
	return nil
}

func mark_acknowleged_message(message_Id int) {
	messages_state[message_Id]--
}

func send_message(message Message) {
	for _, client := range message.recievers {
		connection, err := net.Dial(client.ConnectionType, client.Host+":"+client.Port)
		if err != nil {
			panic(err)
		}
		_, err = connection.Write([]byte(message.message))
		if err != nil {
			fmt.Println("failed to send the message", err)
		}
		fmt.Printf("Sent '%s' to consumer\n", message.message)
		buffer := make([]byte, 1024)
		mLen, err := connection.Read(buffer)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
		}
		fmt.Printf("Acknowleged: %s with message_Id: %d\n", string(buffer[:mLen]), message.message_Id)
		mark_acknowleged_message(message.message_Id)
		connection.Close()
	}
}

func start_server() {
	fmt.Println("Running Broker...")

	server, err := net.Listen(brokerclient.BROKER_TYPE, brokerclient.BROKER_HOST+":"+brokerclient.BROKER_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer server.Close()
	fmt.Println("Listening on " + brokerclient.BROKER_HOST + ":" + brokerclient.BROKER_PORT)
	fmt.Println("Waiting for connections...")
	for {
		connection, err := server.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go processClient(connection)
	}
}

func make_an_unacknowleged_message(number_of_required_acknowlegments int) int {
	for i := 0; i < MESSAGE_STATE_SIZE; i++ {
		if messages_state[i] == 0 {
			messages_state[i] = number_of_required_acknowlegments
			return i
		}
	}
	return -1
}

func wait_for_message_to_be_acknowleged(message_Id int) {
	for messages_state[message_Id] != 0 {
		time.Sleep(1 * time.Second)
	}
}

func find_queue_by_name(name string) *Queue {
	for i := 0; i < len(queues); i++ {
		if queues[i].name == name {
			return &queues[i]
		}
	}
	return nil
}

func find_two_way_queue_by_name(name string) *TwoWayQueue {
	for i := 0; i < len(two_way_queues); i++ {
		if two_way_queues[i].name == name {
			return &two_way_queues[i]
		}
	}
	return nil
}

func find_one_way_queue_to_send_message_in_two_way_queue(two_way_queue *TwoWayQueue, sender Client) (*Queue, error) {
	first_queue := &two_way_queue.one_way_queus[0]
	second_queue := &two_way_queue.one_way_queus[1]
	if len(first_queue.recievers) == 0 || len(second_queue.recievers) == 0 {
		return nil, errors.New("on process is listening on the other side of this queue")
	} else if first_queue.recievers[0].Host == sender.Host && first_queue.recievers[0].Port == sender.Port {
		return second_queue, nil
	} else if second_queue.recievers[0].Host == sender.Host && second_queue.recievers[0].Port == sender.Port {
		return first_queue, nil
	} else {
		return nil, errors.New("this process is not connected to this queue")
	}
}

func add_client(client_data Client) *Client {
	for _, client := range clients {
		if client.Host == client_data.Host && client.Port == client_data.Port {
			return &client
		}
	}
	new_client := Client{
		Host:           client_data.Host,
		Port:           client_data.Port,
		ConnectionType: client_data.ConnectionType,
	}
	clients = append(clients, new_client)
	return &new_client
}

func add_client_to_queue_recievers(client *Client, queue_name string) error {
	queue := find_queue_by_name(queue_name)
	if queue != nil {
		queue.recievers = append(queue.recievers, client)
		return nil
	} else {
		return errors.New("no such queue exists")
	}
}

func add_client_to_two_way_queue(client *Client, queue_name string) error {
	two_way_queue := find_two_way_queue_by_name(queue_name)
	if two_way_queue != nil {
		if len(two_way_queue.one_way_queus[0].recievers) == 0 {
			two_way_queue.one_way_queus[0].recievers = append(two_way_queue.one_way_queus[0].recievers, client)
		} else if len(two_way_queue.one_way_queus[1].recievers) == 0 {
			two_way_queue.one_way_queus[1].recievers = append(two_way_queue.one_way_queus[1].recievers, client)
		} else {
			return errors.New("queue is full")
		}
		return nil
	} else {
		return errors.New("no such queue exists")
	}
}

func main() {
	init_queues()
	go send_messages_in_queues()
	start_server()
}

func processClient(connection net.Conn) {
	defer connection.Close()
	buffer := make([]byte, 2048)
	_, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
	}
	message := bytes.NewBuffer(buffer)
	decoder := gob.NewDecoder(message)
	var request brokerclient.SendMessageRequest
	decoder.Decode(&request)
	if err != nil {
		fmt.Println("Error decoding:", err.Error())
	}

	switch request.TypeOfRequest {
	case brokerclient.SEND_MESSAGE_TO_ONE_WAY_QUEUE:
		handle_send_message_request(request, connection)
	case brokerclient.SUBSCRIBE_TO_QUEUE:
		handle_subscribe_to_queue_request(request, connection)
	case brokerclient.CONNECT_TO_TWO_WAY_QUEUE:
		handle_connect_to_two_way_queue(request, connection)
	case brokerclient.SEND_MESSAGE_TO_TWO_WAY_QUEUE:
		handle_send_message_to_two_way_queue_request(request, connection)
	}
}

func handle_send_message_request(request brokerclient.SendMessageRequest, connection net.Conn) {
	fmt.Printf("message: '%s' for queue: '%s'\n", string(request.Message), request.QueueName)
	queue := find_queue_by_name(request.QueueName)
	if queue == nil {
		fmt.Println("no such queue exists!")
		connection.Write([]byte("Error: no such queue exists!"))
		return
	}
	message_Id := make_an_unacknowleged_message(len(queue.recievers))
	if message_Id == -1 {
		fmt.Println("Maximum number of unacknowleged reached!")
		connection.Write([]byte("Error: Maximum number of unacknowleged reached!"))
		return
	}
	err := add_message_to_queue(queue, request.Message, message_Id)
	if err != nil {
		fmt.Println("Error: " + err.Error())
		connection.Write([]byte("Error: " + err.Error()))
		return
	}
	wait_for_message_to_be_acknowleged(message_Id)
	connection.Write([]byte("message '" + string(request.Message) + "' acknowleged!"))
}

func handle_subscribe_to_queue_request(request brokerclient.SendMessageRequest, connection net.Conn) {
	fmt.Printf(
		"subscribe request from client with address %s:%s for queue '%s'\n",
		request.ClientData.Host,
		request.ClientData.Port,
		request.QueueName,
	)
	client := add_client(Client(request.ClientData))
	err := add_client_to_queue_recievers(client, request.QueueName)
	if err != nil {
		fmt.Println("Error: ", err)
		connection.Write([]byte("Error: " + err.Error()))
		return
	}
	connection.Write([]byte("successfully subscribed to queue: " + request.QueueName))
}

func handle_connect_to_two_way_queue(request brokerclient.SendMessageRequest, connection net.Conn) {
	fmt.Printf(
		"connection request from client with address %s:%s for two way queue '%s'\n",
		request.ClientData.Host,
		request.ClientData.Port,
		request.QueueName,
	)
	client := add_client(Client(request.ClientData))
	err := add_client_to_two_way_queue(client, request.QueueName)
	if err != nil {
		fmt.Println("Error: ", err)
		connection.Write([]byte("Error: " + err.Error()))
		return
	}
	connection.Write([]byte("successfully connected to two way queue: " + request.QueueName))
}

func handle_send_message_to_two_way_queue_request(request brokerclient.SendMessageRequest, connection net.Conn) {
	fmt.Printf("message: '%s' for two way queue: '%s'\n", string(request.Message), request.QueueName)
	two_way_queue := find_two_way_queue_by_name(request.QueueName)
	if two_way_queue == nil {
		fmt.Println("no such queue exists!")
		connection.Write([]byte("Error: no such queue exists!"))
		return
	}
	queue, err := find_one_way_queue_to_send_message_in_two_way_queue(two_way_queue, Client(request.ClientData))
	if err != nil {
		fmt.Println("Error: ", err)
		connection.Write([]byte("Error: " + err.Error()))
		return
	}
	message_Id := make_an_unacknowleged_message(len(queue.recievers))
	if message_Id == -1 {
		fmt.Println("Maximum number of unacknowleged reached!")
		connection.Write([]byte("Error: Maximum number of unacknowleged reached!"))
		return
	}
	err = add_message_to_queue(queue, request.Message, message_Id)
	if err != nil {
		fmt.Println("Error: " + err.Error())
		connection.Write([]byte("Error: " + err.Error()))
		return
	}
	wait_for_message_to_be_acknowleged(message_Id)
	connection.Write([]byte("message '" + string(request.Message) + "' acknowleged!"))
}
