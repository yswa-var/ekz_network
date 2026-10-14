package main

import (
	"fmt"
	"time"
)

func someFunc(num int) {
	fmt.Println(num)
}

func main_goroutine() {
	// go routine make fork of the main()
	go someFunc(1)
	go someFunc(2)
	go someFunc(3)

	time.Sleep(time.Second * 2)

	fmt.Println("Hi")
}

// channels: way to communicate between go routines

func main_channels() {
	myChannel := make(chan string)

	anotherChannel := make(chan string)

	go func() {
		myChannel <- "data"
	}()

	go func() {
		anotherChannel <- ".com"
	}()

	select {
	case msgFromMyChannel := <-myChannel:
		fmt.Println(msgFromMyChannel)
	case msgFromAnotherChannel := <-anotherChannel:
		fmt.Println(msgFromAnotherChannel)
	}
}

func main(){
	// buffer channel: going to have a limited cap of 3
	charChannel := make(chan string, 3)

	
}