package main

import "fmt"

func runDemo() {
	// f1 := Encode([]byte("hi"))

	// reader := bytes.NewReader(f1)

	// fmt.Println(reader)

	// msg1, err := Decode(reader)
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(string(msg1))

	// msg2, err := Decode(reader)
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(string(msg2))

	flow := NewFlowControl(1000)

	fmt.Println(flow.Consume(600)) // true
	fmt.Println(flow.Credit)       // 400

	fmt.Println(flow.Consume(500)) // false
	fmt.Println(flow.Credit)       // 400
}
