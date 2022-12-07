package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/broker/brokerclient"
)

type clientDate struct {
	id              string
	host            string
	port            string
	connection_type string
}

var clients = []clientDate{
	{id: "1", host: "localhost", port: "9001", connection_type: "tcp"},
	{id: "2", host: "localhost", port: "9002", connection_type: "tcp"},
	{id: "3", host: "localhost", port: "9003", connection_type: "tcp"},
}

func main() {
	fmt.Println("select on of these senarios: ")
	fmt.Println("1. send sync message from server to client.")
	fmt.Println("2. send async message from server to client.")
	fmt.Println("3. send sync message from server to client into a queue with zero capacity to check queue overflow")
	fmt.Println("4. send and recieive message via a two way queue.")
	fmt.Println("5. initiate 3 clients and subscribe them all to same queue and see how when server publishes a message all of them will get the message.")
	var response string
	fmt.Scanln(&response)
	switch response {
	case "1":
		run_a_listener()
	case "2":
		run_a_listener()
	case "3":
		run_a_listener()
	case "4":
		test_two_way_queue()
	case "5":
		test_multiple_subscribers_to_a_queue()
	default:
		fmt.Println("invalid senario")
	}
}

func test_multiple_subscribers_to_a_queue() {
	for _, client_data := range clients {
		go create_cient(client_data)
	}
	for {

	}
}

func run_a_listener() {
	consumer := brokerclient.InitConsumer(clients[0].connection_type, clients[0].host, clients[0].port)
	response, err := consumer.SubscribeToQueue("owqueue")
	if err != nil {
		fmt.Println("error in subscribing to queue: ", err)
		return
	}
	fmt.Println("Recieved => ", string(response))
	fmt.Println("Client is listening to the broker...")

	consumer.ReceiveFromBrokerForEver(create_handler(clients[0].id))
}

func test_two_way_queue() {
	consumer := brokerclient.InitConsumer(clients[0].connection_type, clients[0].host, clients[0].port)
	response, err := consumer.SubscribeToTwoWayQueue("twqueue")
	if err != nil {
		fmt.Println("error in subscribing to two way queue: ", err)
		return
	}
	fmt.Println("Received: ", string(response))
	fmt.Println("Client ", clients[0].id, "is listening to the broker...")
	go consumer.ReceiveFromBrokerForEver(create_handler(clients[0].id))
	for {
		time.Sleep(time.Second)
		fmt.Println("Enter a text to send to the other side of queue or enter 'exit' to quit the program!!")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			text := scanner.Text()
			if text == "exit" {
				break
			}
			response, err := consumer.SendToTwoWayQueue("twqueue", []byte(text))
			if err != nil {
				fmt.Println("problem in sending message: ", err)
			}
			fmt.Println(string(response))
		}
	}

}

func create_cient(client_data clientDate) {
	consumer := brokerclient.InitConsumer(client_data.connection_type, client_data.host, client_data.port)
	response, err := consumer.SubscribeToQueue("owqueue")
	if err != nil {
		fmt.Println("error in subscribing to queue: ", err)
		return
	}
	fmt.Println("Client ", client_data.id, " => ", string(response))
	fmt.Println("Client ", client_data.id, "is listening to the broker...")

	consumer.ReceiveFromBrokerForEver(create_handler(client_data.id))
}

func create_handler(client_id string) func(net.Conn) {
	return func(connection net.Conn) {
		buffer := make([]byte, 2048)
		mLen, err := connection.Read(buffer)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
		}
		fmt.Println("Client: ", client_id, " Received: ", string(buffer[:mLen]))
		wait(string(buffer[:mLen]))
		_, err = connection.Write([]byte("message: '" + string(buffer[:mLen]) + "'"))
		if err != nil {
			fmt.Println("Error sending ack: ", err.Error())
		}
		connection.Close()
	}
}

func wait(message string) {
	splited_string := strings.Split(message, " ")
	if splited_string[0] == "wait" && len(splited_string) > 1 {
		seconds, err := strconv.Atoi(splited_string[1])
		if err != nil {
			return
		}
		time.Sleep(time.Duration(seconds) * time.Second)
	}
}
