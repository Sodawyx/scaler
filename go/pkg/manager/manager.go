/*
Copyright 2023 The Alibaba Cloud Serverless Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package manager

import (
	"fmt"
	"github.com/AliyunContainerService/scaler/go/pkg/config"
	"github.com/AliyunContainerService/scaler/go/pkg/model"
	scaler2 "github.com/AliyunContainerService/scaler/go/pkg/scaler"
	pb "github.com/AliyunContainerService/scaler/proto"
	"log"
	"sync"
)

type Manager struct {
	rw         sync.RWMutex
	schedulers map[string]scaler2.Scaler
	config     *config.Config
}

type PreAssignObject struct {
	meta   *model.Meta
	preNum int
}

var nodes1 = PreAssignObject{
	meta: &model.Meta{
		Meta: pb.Meta{
			Key:           "nodes1",
			Runtime:       "go",
			TimeoutInSecs: 627,
			MemoryInMb:    512,
		},
	},
	preNum: 2,
}

var nodes2 = PreAssignObject{
	meta: &model.Meta{
		Meta: pb.Meta{
			Key:           "nodes2",
			Runtime:       "go",
			TimeoutInSecs: 627,
			MemoryInMb:    512,
		},
	},
	preNum: 2,
}

func (m *Manager) PreAssign() {
	var preList []PreAssignObject
	preList = append(preList, nodes1)
	preList = append(preList, nodes2)

	m.rw.Lock()
	for i := range preList {
		log.Printf("%s", preList[i].meta)
		scheduler := scaler2.New(preList[i].meta, m.config)
		go scheduler.PreAssign(preList[i].preNum)
		m.schedulers[preList[i].meta.Key] = scheduler
	}
	m.rw.Unlock()
}

func New(config *config.Config) *Manager {
	manager := &Manager{
		rw:         sync.RWMutex{},
		schedulers: make(map[string]scaler2.Scaler),
		config:     config,
	}
	//manager.PreAssign()

	return manager
}

func (m *Manager) GetOrCreate(metaData *model.Meta) scaler2.Scaler {
	m.rw.RLock()
	if scheduler := m.schedulers[metaData.Key]; scheduler != nil {
		m.rw.RUnlock()
		return scheduler
	}
	m.rw.RUnlock()

	m.rw.Lock()
	if scheduler := m.schedulers[metaData.Key]; scheduler != nil {
		m.rw.Unlock()
		return scheduler
	}
	log.Printf("Create new scaler for app %s", metaData.Key)
	scheduler := scaler2.New(metaData, m.config)
	m.schedulers[metaData.Key] = scheduler
	m.rw.Unlock()
	return scheduler
}

func (m *Manager) Get(metaKey string) (scaler2.Scaler, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()
	if scheduler := m.schedulers[metaKey]; scheduler != nil {
		return scheduler, nil
	}
	return nil, fmt.Errorf("scaler of app: %s not found", metaKey)
}
