package nsqEndpoint

type Conf struct {
	LookUpdHttpAddresses []string
	NSQDTCPAddresses     []string
	NSQDTCPAddress       string
	NSQDHTTPAddress      string
}

func DefaultConf() Conf {
	return Conf{
		LookUpdHttpAddresses: []string{"127.0.0.1:4161"},
		NSQDTCPAddresses:     []string{"127.0.0.1:4150"},
		NSQDTCPAddress:       "127.0.0.1:4150",
		NSQDHTTPAddress:      "127.0.0.1:4151",
	}
}
