package raft

import "log"
import "runtime"
// import "os"
import "fmt"

// Debugging
// const Debug = true
const Debug = false

// //输出到文件中
// func DPrintf(format string, a ...interface{}) {
// 	if Debug {
// 		// 以读写模式创建文件，如果不存在就创建
// 		file, err := os.OpenFile("/home/hd/6.5840/src/raft/log.txt", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
// 		if err != nil {
// 			fmt.Println("打开文件失败:", err)
// 			return
// 		}
// 		defer file.Close()

// 		_, filename, line, _ := runtime.Caller(1)
// 		// log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
// 		fmt.Fprintf(file, "[%v:%v]", filename, line)
// 		fmt.Fprintf(file, format, a...)
// 		fmt.Fprintln(file)
// 	}
// }

//输出到控制台
func DPrintf(format string, a ...interface{}) {
	if Debug {
		_, filename, line, _ := runtime.Caller(1)
		log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
		fmt.Printf("[%v:%v]", filename, line)
		fmt.Printf(format, a...)
		fmt.Println()
	}
}
