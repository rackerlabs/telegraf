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
	"runtime"
)

const (
	KeepaliveSec         = 5
	ReconnectIntervalSec = 5
)

type RemoteConfigConnection struct {
	homebaseAddr string
	identifiers  *Identifiers
	ag           *agent.Agent
}

// Connect is a blocking operation that will connect to the given homebase instance and manage
// inputs with the given Agent instance
func Connect(ag *agent.Agent, homebaseAddr string) *RemoteConfigConnection {
	ourId := uuid.NewV4().String()

	connection := &RemoteConfigConnection{
		homebaseAddr: homebaseAddr,
		identifiers: &Identifiers{
			Region: ag.Config.Tags[telegraf.TagRegion],
			Tenant: ag.Config.Tags[telegraf.TagTenantId],
			Tid:    ourId,
		},
		ag: ag,
	}

	log.Printf("I! Starting remote managed input=%v using %v",
		connection.identifiers, homebaseAddr)

	return connection
}

// Run is a blocking operation that will service incoming managed configurations and report status.
// It will also re-connect when losing connectivity with the far-end.
// A closure of the shutdown channel indicates that the remote connection should be closed and this function conclude.
func (c *RemoteConfigConnection) Run(shutdown chan struct{}) {
	for {
		err := c.connect(shutdown, nil)
		if err == nil {
			// normal shutdown
			return
		}

		log.Println("D! sleeping until next connection attempt")
		time.Sleep(ReconnectIntervalSec * time.Second)
	}
}

func (c *RemoteConfigConnection) identifier() string {
	return c.identifiers.Tid;
}

// RunWithClient is a blocking operation that will service incoming managed configurations and report status.
// Unlike Run, this function will return upon loss of the connection to the far-end.
// A closure of the shutdown channel indicates that the remote connection should be closed and this function conclude.
// It returns an error that caused the connection to be close abnormally or nil if shutdown was indicated.
func (c *RemoteConfigConnection) RunWithClient(shutdown chan struct{}, client TelegrafRemoteClient) error {
	return c.connect(shutdown, client)
}

func (c *RemoteConfigConnection) connect(shutdown chan struct{}, client TelegrafRemoteClient) error {

	if client == nil {
		conn, err := grpc.Dial(c.homebaseAddr, grpc.WithInsecure())
		if err != nil {
			log.Printf("E! Failed to connect to homebase: %v\n%v\n", c.homebaseAddr, err.Error())
			return err
		}
		defer conn.Close()
		client = NewTelegrafRemoteClient(conn)
	}

	ctx := context.Background()
	configStream, err := c.phoneHome(client, ctx)
	if err != nil {
		log.Printf("E! Failed to make initial contact with homebase: \n%s\n", err.Error())
		return err
	}
	log.Printf("D! %v connected and ready to accept managed config\n", c.identifier())

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
				log.Printf("E! While receiving config pack\n%s\n", err.Error())
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
			log.Printf("I! Received config pack: %v", configPack)

			c.processConfigPack(configPack)

		case <-keepaliveTicker.C:
			c.ag.QueryManagedInputs(func(ids []string) {

				state, err := client.ReportState(ctx, &CurrentState{
					Identifiers: c.identifiers, ActiveConfigIds: ids,
				})
				if err == nil {
					log.Printf("D! %v state report %v response, removed=%v, err=%v",
						c.identifier(), ids, state.RemovedId, err)
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
func (c *RemoteConfigConnection) phoneHome(client TelegrafRemoteClient, ctx context.Context) (TelegrafRemote_StartConfigStreamingClient, error) {
	greeting := &Greeting{Identifiers: c.identifiers}

	greeting.NodeTag = make(map[string]string)
	// lower precedence, default tags
	greeting.NodeTag[telegraf.TagOS] = runtime.GOOS
	greeting.NodeTag[telegraf.TagArch] = runtime.GOARCH
	// NOTE 'host' is already populated by the standard config
	for k, v := range c.ag.Config.Tags {
		greeting.NodeTag[k] = v
	}

	return client.StartConfigStreaming(ctx, greeting)
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
			input.SetDefaultTags(c.ag.Config.Tags)

			// Enrich the tags to allow external identification of these managed inputs:
			// http_response,method=GET,managedId=07495f20-e88c-466f-9c8c-78cf54a7d4cc,region=west,telegrafId=c8d6d2df-eb55-462b-aa09-52ac02d78efc,tenantId=ac-1,title=http\ response,server=https://www.rackspace.com response_time=0.167121098,http_response_code=200i,result_type="success" 1500308549000000000

			input.Config.Tags[telegraf.TagManagedId] = newCfg.Id
			input.Config.Tags[telegraf.TagTelegrafId] = c.identifiers.Tid
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
