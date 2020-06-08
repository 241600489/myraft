package main

import (
	"fmt"
	"log"
	"myraft"
	"myraft/config"
	"net"
	"net/rpc"
	"time"
)

const (
	port = ":6007"
)

func main() {
	var configPaths = [3]string{"C:\\study\\myraft\\server.properties",
		"C:\\study\\myraft\\server1.properties", "C:\\study\\myraft\\server2.properties"}
	for _, item := range configPaths {
		go initRaft(item)
	}
	time.Sleep(time.Minute * 10000)

}

func initRaft(configPath string) {
	c, _ := config.NewConfig(configPath)
	raftServer := myraft.NewServer(c)
	_ = rpc.RegisterName("Transport", raftServer.Transport())
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
