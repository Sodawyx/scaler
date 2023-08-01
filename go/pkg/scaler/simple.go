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

package scaler

import (
	"container/list"
	"context"
	"fmt"
	"github.com/AliyunContainerService/scaler/go/pkg/config"
	model2 "github.com/AliyunContainerService/scaler/go/pkg/model"
	platform_client2 "github.com/AliyunContainerService/scaler/go/pkg/platform_client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"math"
	"sync"
	"time"

	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
)

const (
	// CLEANUP_TIME_INVERVAL the time interval to execute a clean-up operation
	CLEANUP_TIME_INVERVAL = 5 * time.Second

	// UPWARD_THRESHOLD the threshold when the request is increasing
	UPWARD_THRESHOLD = 0.2
	// DOWNWARD_THRESHOLD the threshold when the request is declining, unit is MB*s
	DOWNWARD_THRESHOLD = 512

	// EXPAND the expand coefficient
	EXPAND = 1.3
	// DECLINE the decline coefficient
	DECLINE = 0.2
)

type Simple struct {
	config         *config.Config
	metaData       *model2.Meta
	platformClient platform_client2.Client
	mu             sync.Mutex
	wg             sync.WaitGroup
	instances      map[string]*model2.Instance
	idleInstance   *list.List
	// recording the start duration
	executionDuration float32
	startTime         map[string]*time.Time
	// recording the assign time
	assignDuration float32
	// recording the request times in the last time interval
	lastRequestTimes int
	// recording the request times
	requestTimes   int
	lastAssignTime time.Time
}

func New(metaData *model2.Meta, config *config.Config) Scaler {
	client, err := platform_client2.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	scheduler := &Simple{
		config:            config,
		metaData:          metaData,
		platformClient:    client,
		mu:                sync.Mutex{},
		wg:                sync.WaitGroup{},
		instances:         make(map[string]*model2.Instance),
		idleInstance:      list.New(),
		executionDuration: 0,
		startTime:         make(map[string]*time.Time),
		assignDuration:    0,
		lastAssignTime:    time.Now(),
		lastRequestTimes:  -1,
		requestTimes:      0,
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	scheduler.wg.Add(1)
	go func() {
		defer scheduler.wg.Done()
		// only dataset 3 use this strategy
		if len(metaData.Key) == 40 {
			scheduler.CleanUp()
		}

		scheduler.gcLoop()
		log.Printf("gc loop for app: %s is stoped", metaData.Key)
	}()

	return scheduler
}

func (s *Simple) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	start := time.Now()
	instanceId := uuid.New().String()
	defer func() {
		//log.Printf("Assign, request id: %s, instance id: %s, cost %dms", request.RequestId, instanceId, time.Since(start).Milliseconds())
	}()
	//log.Printf("Assign, request id: %s", request.RequestId)
	s.mu.Lock()
	s.requestTimes++
	assignDuration := float32(time.Since(s.lastAssignTime).Milliseconds())
	s.lastAssignTime = start

	// if find the free instance, scheduling this instance for test
	if element := s.idleInstance.Front(); element != nil {
		// average time of the duration between last scheduling and this scheduling
		s.assignDuration = s.assignDuration*0.2 + assignDuration*0.8
		//log.Printf("slot id: %s, assign duration: %f", s.metaData.Key, s.assignDuration)

		instance := element.Value.(*model2.Instance)
		instance.Busy = true
		s.idleInstance.Remove(element)
		// record the instance assigned time
		startTime := time.Now()
		s.startTime[instanceId] = &startTime
		s.mu.Unlock()
		//log.Printf("Assign, request id: %s, instance %s reused", request.RequestId, instance.Id)
		instanceId = instance.Id
		return &pb.AssignReply{
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    instance.Meta.Key,
				InstanceId: instance.Id,
			},
			ErrorMessage: nil,
		}, nil
	}
	s.mu.Unlock()

	//Create new Instance
	resourceConfig := model2.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: request.MetaData.MemoryInMb,
		},
	}
	slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, &resourceConfig)
	if err != nil {
		errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
		log.Printf(errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	meta := &model2.Meta{
		Meta: pb.Meta{
			Key:           request.MetaData.Key,
			Runtime:       request.MetaData.Runtime,
			TimeoutInSecs: request.MetaData.TimeoutInSecs,
		},
	}
	instance, err := s.platformClient.Init(ctx, request.RequestId, instanceId, slot, meta)
	if err != nil {
		errorMessage := fmt.Sprintf("create instance failed with: %s", err.Error())
		log.Printf(errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	//add new instance
	s.mu.Lock()
	instance.Busy = true
	s.instances[instance.Id] = instance

	s.assignDuration = s.assignDuration*0.2 + assignDuration*0.8
	//log.Printf("slot id: %s, assign duration: %f, ---no use old instance", s.metaData.Key, s.assignDuration)
	startTime := time.Now()
	s.startTime[instanceId] = &startTime

	s.mu.Unlock()
	//log.Printf("request id: %s, instance %s for app %s is created, init latency: %dms", request.RequestId, instance.Id, instance.Meta.Key, instance.InitDurationInMs)

	return &pb.AssignReply{
		Status: pb.Status_Ok,
		Assigment: &pb.Assignment{
			RequestId:  request.RequestId,
			MetaKey:    instance.Meta.Key,
			InstanceId: instance.Id,
		},
		ErrorMessage: nil,
	}, nil
}

func (s *Simple) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	if request.Assigment == nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("assignment is nil"))
	}
	reply := &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}
	//start := time.Now()
	instanceId := request.Assigment.InstanceId
	defer func() {
		//log.Printf("Idle, request id: %s, instance: %s, cost %dus", request.Assigment.RequestId, instanceId, time.Since(start).Microseconds())
	}()
	//log.Printf("Idle, request id: %s", request.Assigment.RequestId)
	needDestroy := false
	slotId := ""
	if request.Result != nil && request.Result.NeedDestroy != nil && *request.Result.NeedDestroy {
		needDestroy = true
	}
	defer func() {
		if needDestroy {
			s.deleteSlot(ctx, request.Assigment.RequestId, slotId, instanceId, request.Assigment.MetaKey, "bad instance")
		}
	}()
	//log.Printf("Idle, request id: %s", request.Assigment.RequestId)
	s.mu.Lock()
	defer s.mu.Unlock()

	exeTime := time.Since(*s.startTime[instanceId]).Milliseconds()
	s.executionDuration = s.executionDuration*0.2 + float32(exeTime)*0.8
	//log.Printf("**slot id: %s, exec suration: %f", s.metaData.Key, s.executionDuration)

	if instance := s.instances[instanceId]; instance != nil {
		slotId = instance.Slot.Id
		instance.LastIdleTime = time.Now()
		// add the timeout destroy the instance strategy //
		if s.executionDuration >= 1200 && instance.InitDurationInMs <= 200 && s.assignDuration > 1000*60 {
			needDestroy = true
			delete(s.instances, instanceId)
			//log.Printf("request id %s, instance %s need be destroy", request.Assigment.RequestId, instanceId)
			return reply, nil
		}

		if needDestroy {
			delete(s.instances, instanceId)
			//log.Printf("request id %s, instance %s need be destroy", request.Assigment.RequestId, instanceId)
			return reply, nil
		}

		if instance.Busy == false {
			//log.Printf("request id %s, instance %s already freed", request.Assigment.RequestId, instanceId)
			return reply, nil
		}
		instance.Busy = false
		s.idleInstance.PushFront(instance)
	} else {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}
	return &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}, nil
}

func (s *Simple) deleteSlot(ctx context.Context, requestId, slotId, instanceId, metaKey, reason string) {
	log.Printf("start delete Instance %s (Slot: %s) of app: %s", instanceId, slotId, metaKey)
	if err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason); err != nil {
		log.Printf("delete Instance %s (Slot: %s) of app: %s failed with: %s", instanceId, slotId, metaKey, err.Error())
	}
}

func (s *Simple) gcLoop() {
	log.Printf("gc loop for app: %s is started", s.metaData.Key)
	ticker := time.NewTicker(s.config.GcInterval)
	for range ticker.C {
		for {
			s.mu.Lock()
			if element := s.idleInstance.Back(); element != nil {
				instance := element.Value.(*model2.Instance)
				idleDuration := time.Now().Sub(instance.LastIdleTime)
				if idleDuration > s.config.IdleDurationBeforeGC || s.idleInstance.Len() > 10 {
					//need GC
					s.idleInstance.Remove(element)
					delete(s.instances, instance.Id)
					s.mu.Unlock()
					go func() {
						reason := fmt.Sprintf("Idle duration: %fs, excceed configured duration: %fs", idleDuration.Seconds(), s.config.IdleDurationBeforeGC.Seconds())
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
					}()

					continue
				}
			}
			s.mu.Unlock()
			break
		}
	}
}

// CleanUp clean up the idle instances according to TCP strategy
func (s *Simple) CleanUp() {
	ticker := time.NewTicker(CLEANUP_TIME_INVERVAL)
	for range ticker.C {
		//log.Printf("%s: !!!!! clean up operation !!!!!", s.metaData.Meta.Key)
		s.mu.Lock()
		diff := s.requestTimes - s.lastRequestTimes
		diffMem := math.Abs(float64(s.idleInstance.Len())) * float64(s.metaData.MemoryInMb)
		diffProportion := math.Abs(float64(diff) / float64(s.lastRequestTimes))
		cutProportion := math.Abs(float64(s.requestTimes) / float64(s.lastRequestTimes))
		if s.lastRequestTimes == 0 {
			cutProportion = 0.0
		}
		maxCut := math.Min(cutProportion, DECLINE)

		expect := int(math.Floor(maxCut * float64(s.idleInstance.Len())))
		//log.Printf("%s: request time: %d, last req time: %d, diff: %d, diffMem: %f",
		//s.metaData.Meta.Key, s.requestTimes, s.lastRequestTimes, diff, diffMem)

		if diff > 0 {
			//log.Printf("#####should be increase")
			// request increasing
			// create more slots
			//log.Printf("expand: %s: request time: %d, last req time: %d, diff: %d, diffProp: %f",
			//	s.metaData.Meta.Key, s.requestTimes, s.lastRequestTimes, diff, diffProportion)

			//if diffProportion > UPWARD_THRESHOLD {
			//	//expect := int(EXPAND * float64(s.requestTimes))
			//	expect := int(EXPAND * float64(len(s.instances)))
			//	go s.createBatchSlots(expect)
			//}
		} else if diff <= 0 {
			//log.Printf("#####should be decline")
			log.Printf("decline: %s: request time: %d, last req time: %d, diff: %d, diffMem: %f",
				s.metaData.Meta.Key, s.requestTimes, s.lastRequestTimes, diff, diffMem)
			log.Printf("maxCut is: %f", maxCut)

			// request decreasing
			// delete more slots
			//if s.requestTimes <= len(s.instances) {
			//	expect = len(s.instances)
			//	go s.deleteBatchSlots(expect)
			//} else
			if diffMem > DOWNWARD_THRESHOLD || diffProportion > 0.2 {
				//expect := int(DECLINE * float64(s.requestTimes))
				go s.deleteBatchSlots(expect)
			}

		}
		s.lastRequestTimes = s.requestTimes
		s.requestTimes = 0
		s.mu.Unlock()
	}

}

func (s *Simple) createBatchSlots(expect int) {
	log.Printf("%s: createBatchSlots", s.metaData.Meta.Key)
	startTime := time.Now()
	for {
		if time.Since(startTime) > 40*time.Second {
			log.Printf("time out, can not wait for create more slots")
			break
		}
		if len(s.instances) >= expect {
			log.Printf("**finish create slots")
			break
		}
		log.Printf("%s: instance num: %d, expected num: %d", s.metaData.Meta.Key, len(s.instances), expect)
		resourceConfig := model2.SlotResourceConfig{
			ResourceConfig: pb.ResourceConfig{MemoryInMegabytes: s.metaData.MemoryInMb},
		}

		ctx := context.Background()

		slot, err := s.platformClient.CreateSlot(ctx, uuid.NewString(), &resourceConfig)

		if err != nil {
			errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
			log.Printf(errorMessage)
		}

		meta := &model2.Meta{
			Meta: pb.Meta{
				Key:           s.metaData.Meta.Key,
				Runtime:       s.metaData.Meta.Runtime,
				TimeoutInSecs: s.metaData.Meta.TimeoutInSecs,
			},
		}
		instance, err := s.platformClient.Init(ctx, uuid.NewString(), uuid.New().String(), slot, meta)

		if err != nil {
			errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
			log.Printf(errorMessage)
		}
		s.mu.Lock()
		instance.Busy = false
		instanceStartTime := time.Now()
		s.startTime[instance.Id] = &instanceStartTime
		s.instances[instance.Id] = instance
		s.idleInstance.PushBack(instance)
		s.mu.Unlock()
		log.Printf("** create success **")
		continue
	}
}

func (s *Simple) deleteBatchSlots(expect int) {
	log.Printf("%s: deleteBatchSlots, expect value is: %d， idle num is: %d", s.metaData.Meta.Key, expect, s.idleInstance.Len())
	for {
		//s.mu.Lock()
		//log.Printf("aaa")
		//log.Printf("%s: instance num: %d, expected num: %d", s.metaData.Meta.Key, len(s.instances), int(DECLINE*float64(s.requestTimes)))
		if s.idleInstance.Len() <= expect {
			log.Printf("**finish delete slots")
			break
		}
		if element := s.idleInstance.Back(); element != nil {
			log.Printf("%s: instance num: %d, expected num: %d", s.metaData.Meta.Key, s.idleInstance.Len(), expect)
			instance := element.Value.(*model2.Instance)
			s.idleInstance.Remove(element)
			delete(s.instances, instance.Id)
			//s.mu.Unlock()
			go func() {
				reason := fmt.Sprintf("delete the slots according to the clean up strategy")
				ctx := context.Background()
				ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
			}()
			log.Printf("** delete success **")
			continue
		} else {
			log.Printf("do not have more idel instances")
			break
		}
		//s.mu.Unlock()

	}
}

func (s *Simple) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		TotalInstance:     len(s.instances),
		TotalIdleInstance: s.idleInstance.Len(),
	}
}
