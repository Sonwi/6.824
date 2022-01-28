package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ReduceKv struct {
	Key   string
	Value []string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args1 := ReqArgs{ReqNumber: 1}
		reply1 := ReqReply{}

		if ok := call("Coordinator.HandWorkerReq", &args1, &reply1); ok {
			//deal map task
			if reply1.TypeName == "map" {
				intermediate := []KeyValue{}
				filename := reply1.Content[0]
				NReduce := reply1.NReduce
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)

				sort.Sort(ByKey(intermediate))

				outNames := make([]string, NReduce)
				tempFiles := make([]*os.File, NReduce)

				for i := range outNames {
					outNames[i] = "mr-map-out-" + strconv.Itoa(reply1.Idx) + "-" + strconv.Itoa(i)
				}

				for i := range tempFiles {
					tempFiles[i], err = ioutil.TempFile(".", "out*")
					if err != nil {
						log.Fatal("creat tempfile fail")
					}
				}

				i := 0
				reduceInput := make([][]ReduceKv, NReduce)
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}

					key := intermediate[i].Key

					idx := ihash(key) % NReduce
					reduceInput[idx] = append(reduceInput[idx], ReduceKv{Key: key, Value: values})

					// output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}

				for i := range reduceInput {
					enc := json.NewEncoder(tempFiles[i])
					for _, kv := range reduceInput[i] {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("cannot write json %v", i)
						}
					}
				}

				//rename
				for i := range tempFiles {
					_, err := os.Stat(outNames[i])
					if os.IsNotExist(err) {
						os.Rename(tempFiles[i].Name(), outNames[i])
						if i == len(tempFiles)-1 {
							//call finish info
							args := FinishReq{TaskStr: "map", Ret: outNames, Idx: reply1.Idx}
							reply := FinishReply{}
							call("Coordinator.HandFinishInfo", &args, &reply)
							// if ok {
							// 	log.Println("finish map job success")
							// } else {
							// 	log.Println("finish map job fail")
							// }
						} else {
							os.Remove(tempFiles[i].Name())
						}
					} else {
						break
					}
				}

			} else if reply1.TypeName == "reduce" {
				idx := reply1.Idx
				inputFileNames := reply1.Content

				oTmpFile, _ := ioutil.TempFile(".", "reout*.txt")
				kvaMap := make(map[string]*ReduceKv)

				// log.Printf("reduce work %v file length %v", idx, len(inputFileNames))

				for _, filename := range inputFileNames {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}

					dec := json.NewDecoder(file)

					// for i := 0; i < len(inputFileNames); i++ {

					for {
						var kv ReduceKv
						if err := dec.Decode(&kv); err != nil {
							break
						}
						if kvaMap[kv.Key] == nil {
							kvaMap[kv.Key] = &kv
						} else {
							kvaMap[kv.Key].Value = append(kvaMap[kv.Key].Value, kv.Value...)
						}
						// output := reducef(kv.Key, kv.Value)
						// fmt.Fprintf(oTmpFile, "%v %v\n", kv.Key, output)
					}

					// }
				}
				for _, kv := range kvaMap {
					output := reducef(kv.Key, kv.Value)
					fmt.Fprintf(oTmpFile, "%v %v\n", kv.Key, output)
				}
				oname := "mr-out-" + strconv.Itoa(idx)
				retName := []string{}
				_, err := os.Stat(oname)
				if os.IsNotExist(err) {
					os.Rename(oTmpFile.Name(), oname)
					args := FinishReq{TaskStr: "reduce", Ret: append(retName, oname), Idx: idx}
					reply := FinishReply{}
					call("Coordinator.HandFinishInfo", &args, &reply)
					// if ok {
					// 	log.Println("finish reduce job success")
					// } else {
					// 	log.Println("finish reduce job fail")
					// }
				} else {
					os.Remove(oTmpFile.Name())
				}
			} //else if reply1.TypeName == "allinprogress" {
			// 	log.Println("all in progress wait")
			// } else {
			// 	// time.Sleep(10 * time.Second)
			// 	log.Println("all job complete, wait to exit")
			// 	// return
			// }
		} else {
			break
		}
		// time.Sleep(1 * time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
