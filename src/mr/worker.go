package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// debug helper function
func debug(message string) {
	// get current process id
	pid := os.Getpid()

	// format the log message
	logMessage := fmt.Sprintf("[%d] %s", pid, message)

	logFile, _ := os.Open("out.log")
	defer logFile.Close()
	// log.SetOutput(logFile)

	// print to stdout
	fmt.Print(logMessage)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// debug(fmt.Sprintf("begin worker"))
	for {
		// ask for job
		jobInfo, ok := AskForJob()
		// debug(fmt.Sprintf("jobinfo: %+v\n", jobInfo))
		if !ok {
			break
		}
		if jobInfo.ReplyStatus == Finished || jobInfo.ReplyStatus == Failed {
			break
		}
		if jobInfo.ReplyStatus == Waiting {
			time.Sleep(10 * time.Second)
			continue
		}

		// map job
		if jobInfo.JobType == Map {
			// open the file and read the content
			file, err := os.Open(jobInfo.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", jobInfo.FileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", jobInfo.FileName)
			}
			file.Close()
			KVs := mapf(jobInfo.FileName, string(content))

			intermediateArr := make([][]KeyValue, jobInfo.FileCount)

			for _, kv := range KVs {
				pos := ihash(kv.Key) % jobInfo.FileCount
				intermediateArr[pos] = append(intermediateArr[pos], kv)
			}

			// persist the intermediate file
			for id, intermediateKV := range intermediateArr {
				// the intermediate file name format is `mr-X-Y`, where X is the Map task number, and Y is the reduce task number.
				fileName := "mr-" + strconv.Itoa(jobInfo.JobId) + "-" + strconv.Itoa(id)
				// intermediateFile, err := os.Create(fileName)
				tempFile, err := os.CreateTemp("", "intermediate")
				if err != nil {
					log.Fatalf("Woker create temp file fail, error: %v", err)
				}
				defer os.Remove(tempFile.Name())

				enc := json.NewEncoder(tempFile)
				for _, kv := range intermediateKV {
					err = enc.Encode(&kv)
					if err != nil {
						log.Fatalf("Encode kv %v fail, error: %v", err)
					}
				}

				// rename the file
				err = os.Rename(tempFile.Name(), fileName)
				if err != nil {
					log.Fatalf("Worker create intermediate file with name %s fail, error: %v", fileName, err)
				}
				tempFile.Close()
			}
			// debug(fmt.Sprintf("finish map job\n"))
		} else { // reduce job
			intermediateKV := make([]KeyValue, 0)
			for id := 0; id < jobInfo.FileCount; id++ {
				fileName := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(jobInfo.JobId)
				intermediateFile, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("Worker cannot open the intermediate file with name %s, error: %v\n", fileName, err)
				}

				dec := json.NewDecoder(intermediateFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediateKV = append(intermediateKV, kv)
				}

				intermediateFile.Close()
			}

			sort.Sort(ByKey(intermediateKV))
			outFileName := "mr-out-" + strconv.Itoa(jobInfo.JobId)
			tempFile, err := os.CreateTemp("", "output")
			if err != nil {
				log.Fatalf("Woker create temp output file fail, error: %v", err)
			}
			defer os.Remove(tempFile.Name())

			// call Reduce on each distinct key in intermediateKV[],
			// and print the result to outputFile.
			i := 0
			for i < len(intermediateKV) {
				j := i + 1
				for j < len(intermediateKV) && intermediateKV[j].Key == intermediateKV[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediateKV[k].Value)
				}
				output := reducef(intermediateKV[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", intermediateKV[i].Key, output)

				i = j
			}

			// rename the file
			err = os.Rename(tempFile.Name(), outFileName)
			if err != nil {
				log.Fatalf("create reduce output file %s fail, error: %v", outFileName, err)
			}
			tempFile.Close()

			// debug(fmt.Sprintf("finish reduce job\n"))
		}

		// send ack to the coordinator
		SendAck(jobInfo.FileName)
		//if jobInfo.JobType == Reduce {
		//	debug(fmt.Sprintf("send reduce ack info: %+v\n", jobInfo.FileName))
		//} else {
		//	debug(fmt.Sprintf("send map ack info: %+v\n", jobInfo.FileName))
		//}
	}
}

// AskForJob worker ask the coordinator for job assignment
func AskForJob() (*RequestCoordinatorReply, bool) {
	// declare an argument
	args := RequestWorkerArgs{
		ArgType: Request,
	}

	// declare a reply structure
	reply := &RequestCoordinatorReply{}

	// send the rpc request
	ok := call("Coordinator.ServeMapReduce", &args, reply)
	return reply, ok
}

// SendAck send acknowledgement to the coordinator
func SendAck(fileName string) (*FinishCoordinatorReply, bool) {
	// declare an argument
	args := FinishWorkerArgs{
		ArgType:  Finish,
		FileName: fileName,
	}

	// declare a reply structure
	reply := &FinishCoordinatorReply{}

	// send the rpc request
	ok := call("Coordinator.ServeACK", &args, reply)
	return reply, ok
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
