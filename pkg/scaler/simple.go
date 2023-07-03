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
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/AliyunContainerService/scaler/pkg/config"
	"github.com/AliyunContainerService/scaler/pkg/model"
	"github.com/AliyunContainerService/scaler/pkg/platform_client"
	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
)

type Simple struct {
	config         *config.Config
	metaData       *model.Meta
	platformClient platform_client.Client
	mu             sync.Mutex
	wg             sync.WaitGroup
	instances      map[string]*model.Instance
	idleInstance   *list.List
	currentReq     int
	currentSlot    int
}

func New(metaData *model.Meta, config *config.Config) Scaler {
	client, err := platform_client.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	scheduler := &Simple{
		config:         config,
		metaData:       metaData,
		platformClient: client,
		mu:             sync.Mutex{},
		wg:             sync.WaitGroup{},
		instances:      make(map[string]*model.Instance),
		idleInstance:   list.New(),
		currentReq:     0,
		currentSlot:    0,
	}
	scheduler.wg.Add(1)
	go func() {
		defer scheduler.wg.Done()
		scheduler.gcLoop()
	}()

	return scheduler
}

func (s *Simple) TryGetIdleSlot() (*model.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if element := s.idleInstance.Front(); element != nil {
		instance := element.Value.(*model.Instance)
		s.idleInstance.Remove(element)
		return instance, nil
	}
	return nil, nil
}

func (s *Simple) ConstructSlot(ctx context.Context, request *pb.AssignRequest) (*model.Instance, error) {
	//Create new Instance
	resourceConfig := model.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: request.MetaData.MemoryInMb,
		},
	}
	slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, &resourceConfig)
	if err != nil {
		errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	meta := &model.Meta{
		Meta: pb.Meta{
			Key:           request.MetaData.Key,
			Runtime:       request.MetaData.Runtime,
			TimeoutInSecs: request.MetaData.TimeoutInSecs,
		},
	}
	instanceId := uuid.New().String()
	instance, err := s.platformClient.Init(ctx, request.RequestId, instanceId, slot, meta)
	if err != nil {
		errorMessage := fmt.Sprintf("create instance failed with: %s", err.Error())
		return nil, status.Errorf(codes.Internal, errorMessage)
	}
	return instance, nil
}

func (s *Simple) ConstructAndPushSlotToQueue(ctx context.Context, request *pb.AssignRequest) {
	s.UpdateCurrentSlot(1, true)

	instance, err := s.ConstructSlot(ctx, request)
	if err == nil {
		s.mu.Lock()
		s.idleInstance.PushFront(instance)
		s.mu.Unlock()
	} else {
		// log.Printf("ConstructAndPushSlotToQueue failed, request id: %s, err: %s", request.RequestId, err.Error())
	}
}

func (s *Simple) UpdateCurrentSlot(delta int, lock bool) {
	if lock {
		s.mu.Lock()
	}
	s.currentSlot += delta
	if lock {
		s.mu.Unlock()
	}
}

func (s *Simple) UpdateCurrentReq(delta int, lock bool) {
	if lock {
		s.mu.Lock()
	}
	s.currentReq += delta
	if lock {
		s.mu.Unlock()
	}
}

func (s *Simple) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	instance, err := s.TryGetIdleSlot()
	if err != nil {
		errorMessage := fmt.Sprintf("TryGetIdleSlot instance failed with: %s", err.Error())
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	s.UpdateCurrentReq(1, true)
	defer s.UpdateCurrentReq(-1, true)

	if instance == nil {
		constructDone := false
		mtx := new(sync.Mutex)

		go func() {
			s.ConstructAndPushSlotToQueue(ctx, request)
			mtx.Lock()
			constructDone = true
			mtx.Unlock()
		}()

		waitTimeMs := s.config.WaitTimeInitial
		for {
			instance, err = s.TryGetIdleSlot()
			if err != nil {
				errorMessage := fmt.Sprintf("TryGetIdleSlot instance failed with: %s", err.Error())
				return nil, status.Errorf(codes.Internal, errorMessage)
			}
			if instance != nil {
				break
			}

			mtx.Lock()
			if constructDone {
				mtx.Unlock()
				errorMessage := fmt.Sprintf("ConstructAndPushSlotToQueue failed, request id: %s", request.RequestId)
				return nil, status.Errorf(codes.Internal, errorMessage)
			}
			mtx.Unlock()

			time.Sleep(waitTimeMs)
		}
	}

	//add new instance
	s.mu.Lock()
	instance.Busy = true
	s.instances[instance.Id] = instance
	s.mu.Unlock()

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
	instanceId := request.Assigment.InstanceId

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

	s.mu.Lock()
	defer s.mu.Unlock()
	if instance := s.instances[instanceId]; instance != nil {
		slotId = instance.Slot.Id
		instance.LastIdleTime = time.Now()
		if needDestroy {
			return reply, nil
		}

		if instance.Busy == false {
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
	if err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason); err != nil {
	}
}

func (s *Simple) gcLoop() {
	ticker := time.NewTicker(s.config.GcInterval)
	for range ticker.C {
		for {
			s.mu.Lock()
			if element := s.idleInstance.Back(); element != nil {
				instance := element.Value.(*model.Instance)
				idleDuration := time.Now().Sub(instance.LastIdleTime)
				if idleDuration > s.config.IdleDurationBeforeGC {
					s.UpdateCurrentSlot(-1, false)
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

func (s *Simple) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		TotalInstance:     len(s.instances),
		TotalIdleInstance: s.idleInstance.Len(),
	}
}
