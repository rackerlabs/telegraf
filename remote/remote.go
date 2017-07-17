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
	"github.com/influxdata/telegraf"
	"errors"
)

const KeepaliveSec = 5

type RemoteConfigConnection struct {
	homebaseAddr string
	region       string
	ourId        string
	ag           *agent.Agent
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

	return &RemoteConfigConnection{
		homebaseAddr: homebaseAddr,
		region:       region,
		ourId:        ourId,
		ag:           ag,
	}
}

// Run is a blocking operation that will service incoming managed configurations and report status.
func (c *RemoteConfigConnection) Run(shutdown chan struct{}) {
	for {
		err := c.connect(shutdown)
		if err == nil {
			// normal shutdown
			return
		}

		log.Println("DEBUG sleeping until next connection attempt")
		time.Sleep(5 * time.Second)
	}
}

func (c *RemoteConfigConnection) connect(shutdown chan struct{}) error {

	conn, err := grpc.Dial(c.homebaseAddr, grpc.WithInsecure())
	if err != nil {
		log.Printf("E! Failed to connect to homebase: %v\n%v\n", c.homebaseAddr, err.Error())
		return err
	}
	defer conn.Close()

	client := NewTelegrafRemoteClient(conn)
	ctx := context.Background()
	configStream, err := client.StartConfigStreaming(ctx, &Greeting{Tid: c.ourId, Region: c.region})
	if err != nil {
		log.Printf("E! Failed to make initial contact with homebase: \n%s\n", err.Error())
		return err
	}
	log.Printf("DEBUG %v connected and ready to accept managed config\n", c.ourId)

	configPacks := make(chan *ConfigPack, 1)

	farendAlive := make(chan struct{}, 1)

	// Convert blocking grpc stream recv into channel receive
	go func() {
		defer close(farendAlive)
		for {

			configPack, err := configStream.Recv()
			if err == io.EOF {
				log.Printf("E! Lost connection to homebase %v", c.homebaseAddr)
				return
			}

			if err != nil {
				log.Printf("While receiving config pack\n%s\n", err.Error())
				return
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
					log.Printf("DEBUG %v state report %v response, removed=%v, err=%v",
						c.ourId, ids, state.RemovedId, err)
					for _, removedId := range state.RemovedId {
						c.ag.RemoveManagedInput(removedId)
					}
				} else {
					log.Printf("E! While reporting state\n%s\n", err.Error())
				}
			})

		case <-farendAlive:
			return errors.New("Farend closed down")

		case <-shutdown:
			return nil
		}
	}

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

		if len(telegrafCfg.Inputs) >= 1 {
			input := telegrafCfg.Inputs[0]

			// Enrich the tags to allow external identification of these managed inputs:
			// http_response,method=GET,managedId=07495f20-e88c-466f-9c8c-78cf54a7d4cc,region=west,telegrafId=c8d6d2df-eb55-462b-aa09-52ac02d78efc,tenantId=ac-1,title=http\ response,server=https://www.rackspace.com response_time=0.167121098,http_response_code=200i,result_type="success" 1500308549000000000

			input.Config.Tags[telegraf.TagManagedId] = newCfg.Id
			input.Config.Tags[telegraf.TagRegion] = c.region
			input.Config.Tags[telegraf.TagTelegrafId] = c.ourId
			if newCfg.TenantId != "" {
				input.Config.Tags[telegraf.TagTenantId] = newCfg.TenantId
			}
			if newCfg.Title != "" {
				input.Config.Tags[telegraf.TagTitle] = newCfg.Title
			}
			c.ag.AddManagedInput(newCfg.Id, input)
		} else {
			log.Printf("W! No inputs in given config pack")
		}
	}
}
