package main

import (
	"fmt"
	"log"
	"myraft/config"
	"myraft/raft"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

const (
	port = ":6007"
)

func main() {
	var configPaths = [3]string{"D:\\projects\\myraft\\server.properties",
		"D:\\projects\\myraft\\server1.properties", "D:\\projects\\myraft\\server2.properties"}
	for _, item := range configPaths {
		go initRaft(item)
	}
	time.Sleep(time.Minute * 10000)

}

func initRaft(configPath string) {
	c, _ := config.NewConfig(configPath)
	raftServer := raft.NewServer(c)
	_ = rpc.RegisterName("Transport"+strconv.FormatUint(raftServer.ID(), 10), raftServer.Transport())
	lis, e := net.Listen("tcp", c.LocalAddr)

	if e != nil {
		log.Fatal(fmt.Errorf("start fail:%s", e.Error()))
		return
	}
	for {
		conn, _ := lis.Accept()
		go rpc.ServeConn(conn)
	}

}
