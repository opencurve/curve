package daemon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type Task struct {
	ID         int      `json:"ID"`
	Path       string   `json:"Path"`
	Args       []string `json:"Args"`
	Env        []string `json:"Env"`
	Dir        string   `json:"Dir"`
	OutputPath string   `json:"OutputPath"`
	InputPath  string   `json:"InputPath"`
}

func NewTask(str []byte) *Task {
	task := Task{}
	json.Unmarshal(str, &task)
	return &task
}

func (task *Task) Run() error {
	cmd := exec.Command(task.Path, task.Args...)
	if task.InputPath != "" {
		inputData, err := ioutil.ReadFile(task.InputPath)
		if err != nil {
			return err
		}
		cmd.Stdin = strings.NewReader(string(inputData))
	}
	var out bytes.Buffer
	defer func() {
		if task.OutputPath != "" {
			ioutil.WriteFile(task.OutputPath, out.Bytes(), 0644)
		}
	}()
	cmd.Stdout = &out
	cmd.Env = append(cmd.Env, task.Env...)
	err := cmd.Run()
	fmt.Printf("cmd:\n%+v\nout:\n%s\n-----------\n", *task, out.String())
	return err
}

func getFileList(path string) []string {
	var fileList []string
	fi, err := os.Stat(path)
	if err != nil || !fi.IsDir() {
		return fileList
	}
	filepath.Walk(path, func(path string, info fs.FileInfo, err error) error {
		if !info.IsDir() {
			fileList = append(fileList, path)
		}
		return nil
	})

	return fileList
}

const (
	WORK_DIRECTORY = "/curve/init.d/"
)

func GetTasks() []*Task {
	fileList := getFileList(WORK_DIRECTORY)
	fmt.Println("fileList:", fileList)
	var tasks []*Task
	for _, file := range fileList {
		fileData, err := ioutil.ReadFile(file)
		if err == nil {
			task := NewTask(fileData)
			tasks = append(tasks, task)
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID < tasks[j].ID
	})
	return tasks
}

func (task *Task)Write(path string) {
	b, err := json.Marshal(task)
	if err != nil {
		return
	}
	ioutil.WriteFile(path, b, 0644)
}
