package config

type RaftConfig struct {
	LocalAddr   string //类似 ‘127.0.0.1:6379’
	ClusterAddr string //集群内地址 127.0.0.1:6379,127.0.0.1:6379
}

func NewConfig(configPath string) (rf *RaftConfig, e error) {
	rf = &RaftConfig{}
	configKv := InitConfig(configPath)
	rf.LocalAddr = configKv["localAddr"]
	rf.ClusterAddr = configKv["clusterAddr"]
	return rf, nil

}
