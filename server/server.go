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

// this client information is only used when we want to test the two way queue
type clientDate struct {
	id              string
	host            string
	port            string
	connection_type string
}

var clients = []clientDate{
	{id: "1", host: "localhost", port: "9005", connection_type: "tcp"},
}

func send_sync_message(message string, queue_name string) {
	publisher := brokerclient.InitPublisher()
	response, err := publisher.SendSyncMessage([]byte(message), queue_name)
	if err != nil {
		fmt.Println("problem in sending message: ", err)
	}
	fmt.Println(string(response))
}

func send_async_message(message string, queue_name string) {
	publisher := brokerclient.InitPublisher()
	response, reply, err := publisher.SendAsyncMessage([]byte(message), queue_name)
	if err != nil {
		fmt.Println("problem in sending message: ", err)
	}
	fmt.Println(string(response))
	fmt.Println(<-reply)
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
		time.Sleep(1 * time.Second)
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

func send_sync_message_to_broker() {
	fmt.Println("Ready to send message to the broker...")
	for {
		fmt.Println("Enter a message to send to the broker...")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		input := scanner.Text()
		send_sync_message(input, "owqueue")
	}
}

func send_async_message_to_broker() {
	fmt.Println("Ready to send message to the broker...")
	for {
		fmt.Println("Enter a message to send to the broker...")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		input := scanner.Text()
		send_async_message(input, "owqueue")
	}
}

func send_to_a_queue_with_zero_capacity_to_check_queue_overflow() {
	fmt.Println("Ready to send message to the broker...")
	for {
		fmt.Println("Enter a message to send to the broker...")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		input := scanner.Text()
		send_sync_message(input, "zerocapacityqueue")
	}
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
		send_sync_message_to_broker()
	case "2":
		send_async_message_to_broker()
	case "3":
		send_to_a_queue_with_zero_capacity_to_check_queue_overflow()
	case "4":
		test_two_way_queue()
	case "5":
		send_sync_message_to_broker()
	default:
		fmt.Println("invalid senario")
	}
}
