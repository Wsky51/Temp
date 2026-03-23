package raft

import (
	//	"bytes"
	"fmt"
	"log"
	"time"
	"os"
	"strconv"
)

var debugStart time.Time
var debugVerbosity int

type logTopic string

const (
    dClient  logTopic = "CLNT"  // 客户端相关
    dCommit  logTopic = "CMIT"  // 提交相关
    dDrop    logTopic = "DROP"  // 消息丢弃相关
    dError   logTopic = "ERRO"  // 错误相关
    dInfo    logTopic = "INFO"  // 信息相关
    dLeader  logTopic = "LEAD"  // 领导者相关
    dLog     logTopic = "LOG1"  // 日志1（一级日志）
    dLog2    logTopic = "LOG2"  // 日志2（二级日志）
    dPersist logTopic = "PERS"  // 持久化相关
    dSnap    logTopic = "SNAP"  // 快照相关
    dTerm    logTopic = "TERM"  // 任期相关
    dTest    logTopic = "TEST"  // 测试相关
    dTimer   logTopic = "TIMR"  // 定时器相关
    dTrace   logTopic = "TRCE"  // 跟踪相关
    dVote    logTopic = "VOTE"  // 投票相关
    dWarn    logTopic = "WARN"  // 警告相关
)

func getVerbosity() int {
    v := os.Getenv("VERBOSE")
    level := 0
    if v != "" {
       var err error
       level, err = strconv.Atoi(v)
       if err != nil {
          log.Fatalf("Invalid verbosity %v", v)
       }
    }
    return level
}

func init() {
    debugVerbosity = getVerbosity()
    debugStart = time.Now()
    // 禁用日志中的日期和时间输出
    log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugPretty(topic logTopic, format string, a ...interface{}) {
    if debugVerbosity >= 1 {
        // 计算从程序启动到当前的微秒数，并转换为毫秒（保留一位小数）
        time := time.Since(debugStart).Microseconds()
        time /= 100
        // 构建日志前缀：6位毫秒数 + 主题
        prefix := fmt.Sprintf("%06d %v ", time, string(topic))
        // 将前缀添加到日志格式中
        format = prefix + format
        // 输出日志
        log.Printf(format, a...)
    }
}
