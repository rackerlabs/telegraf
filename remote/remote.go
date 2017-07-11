//
// Copyright 2017 Rackspace
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS-IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package remote

import (
	"io"
	"github.com/satori/go.uuid"
	"google.golang.org/grpc"
	"log"
	"context"
	"github.com/influxdata/telegraf/agent"
	"github.com/influxdata/telegraf/internal/config"
	"strings"
	"time"
)

const KeepaliveSec = 10

type RemoteConfigConnection struct {
	homebaseAddr string
	region       string
	ourId        string
	ag           *agent.Agent
	conn         *grpc.ClientConn
}

// Connect is a blocking operation that will connect to the given homebase instance and manage
// inputs with the given Agent instance
func Connect(ag *agent.Agent, homebaseAddr string, region string) *RemoteConfigConnection {
	ourId := uuid.NewV4().String()
	if region == "" {
		region = "default"
	}

	log.Printf("Starting remote managed input in region %v using %v as %v",
		region, homebaseAddr, ourId)


	conn, err := grpc.Dial(homebaseAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("E! Failed to connect to homebase: %v\n%v\n", homebaseAddr, err.Error())
		return nil
	}

	return &RemoteConfigConnection{
		homebaseAddr: homebaseAddr,
		region:       region,
		ourId:        ourId,
		ag:           ag,
		conn:         conn,
	}
}

// Run is a blocking operation that will service incoming managed configurations and report status.
func (c *RemoteConfigConnection) Run(shutdown chan struct{}) {
	defer c.Close()

	client := NewTelegrafRemoteClient(c.conn)
	ctx := context.Background()
	configStream, err := client.StartConfigStreaming(ctx, &Greeting{Tid: c.ourId, Region: c.region})
	if err != nil {
		log.Fatalf("E! Failed to make initial contact with homebase: \n%s\n", err.Error())
	}

	configPacks := make(chan *ConfigPack, 1)

	// Convert blocking grpc stream recv into channel receive
	go func() {
		for {

			configPack, err := configStream.Recv()
			if err == io.EOF {
				log.Printf("E! Lost connection to homebase %v", c.homebaseAddr)
				break
			}

			if err != nil {
				log.Fatalf("While receiving config pack\n%s\n", err.Error())
			}

			configPacks <- configPack
		}
	}()

	keepaliveTicker := time.NewTicker(KeepaliveSec * time.Second)

	// Multiplex on shutdown and I/O activities
	for {
		select {
		case configPack := <-configPacks:
			log.Printf("Received config pack: %v", configPack)

			c.processConfigPack(configPack)

		case <-keepaliveTicker.C:
			c.ag.QueryManagedInputs(func(ids []string) {

				state, err := client.ReportState(ctx, &CurrentState{Tid: c.ourId, Region: c.region, ActiveConfigIds: ids})
				if err == nil {
					for _, removedId := range state.RemovedId {
						c.ag.RemoveManagedInput(removedId)
					}
				} else {
					log.Printf("E! While reporting state\n%s\n", err.Error())
				}
			})

		case <-shutdown:
			return
		}
	}

}

func (c *RemoteConfigConnection) Close() {
	c.conn.Close()
}

func (c *RemoteConfigConnection) processConfigPack(configPack *ConfigPack) {

	// NEW
	for _, newCfg := range configPack.New {
		telegrafCfg := config.NewConfig()
		defnReader := strings.NewReader(newCfg.Definition)

		err := telegrafCfg.Load(c.homebaseAddr, defnReader)
		if err != nil {
			log.Printf("E! Failed to load configuration with ID %s\n%s\n",
				newCfg.Id, err.Error())
			continue
		}

		if len(telegrafCfg.Inputs) != 1 {
			log.Printf("E! Expected one and only one input configuration %s\n",
				newCfg.Id)
			continue
		}

		c.ag.AddManagedInput(newCfg.Id, telegrafCfg.Inputs[0])
	}
}
