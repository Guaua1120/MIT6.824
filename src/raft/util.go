package raft

import "log"
import "runtime"

// Debugging
// const Debug = true
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		_, file, line, _ := runtime.Caller(1)
		// log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
		log.Printf("[%v:%v]", file, line)
		log.Printf(format, a...)
	}
}
