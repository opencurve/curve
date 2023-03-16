package daemon

import "fmt"

func Execute() {
	tasks := GetTasks()
	for _, t := range tasks {
		err := t.Run()
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}
