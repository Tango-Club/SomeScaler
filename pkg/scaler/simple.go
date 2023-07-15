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

	"github.com/wangjia184/sortedset"
)

type Simple struct {
	config         *config.Config
	metaData       *model.Meta
	platformClient platform_client.Client
	mu             sync.Mutex
	muExpand       sync.Mutex
	wg             sync.WaitGroup
	instances      map[string]*model.Instance
	idleInstance   *list.List
	lastAssignTime time.Time
	deltaTimes     sortedset.SortedSet
	durationTimes  sortedset.SortedSet
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
		lastAssignTime: time.Now(),
		deltaTimes:     *sortedset.New(),
		durationTimes:  *sortedset.New(),
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

func (s *Simple) expectSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.durationTimes.GetCount() == 0 || s.deltaTimes.GetCount() == 0 {
		return 1
	}
	Q := s.durationTimes.GetByRank(s.durationTimes.GetCount()/2, false).Score()
	D := s.deltaTimes.GetByRank(s.deltaTimes.GetCount()/2, false).Score()
	if D == 0 {
		return 1
	}
	result := Q/D + 1
	if result > 50 {
		result = 50
	}
	return int(result)
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
	instance, err := s.ConstructSlot(ctx, request)
	if err == nil {
		s.mu.Lock()
		s.idleInstance.PushFront(instance)
		s.mu.Unlock()
	} else {
		// log.Printf("ConstructAndPushSlotToQueue failed, request id: %s, err: %s", request.RequestId, err.Error())
	}
}

func Pow2Roundup(x int) int {
	if x <= 1 {
		return 1
	}
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	return x + 1
}

func (s *Simple) ExpandSlots(ctx context.Context, request *pb.AssignRequest) {
	s.muExpand.Lock()
	defer s.muExpand.Unlock()

	needNewSlot := s.expectSize() - len(s.instances)
	if needNewSlot <= 0 {
		return
	}

	log.Printf("expand slot, request id: %s, new slot: %d, now slot: %d", request.MetaData.GetKey(), needNewSlot, len(s.instances))

	wg := sync.WaitGroup{}
	wg.Add(needNewSlot)
	for i := 0; i < needNewSlot; i++ {
		go func() {
			s.ConstructAndPushSlotToQueue(ctx, request)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *Simple) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	s.mu.Lock()
	s.deltaTimes.AddOrUpdate(request.RequestId, sortedset.SCORE(time.Now().UnixMilli()-s.lastAssignTime.UnixMilli()), "")
	s.lastAssignTime = time.Now()
	s.mu.Unlock()

	constructDone := false

	go func() {
		s.ExpandSlots(ctx, request)
		constructDone = true
	}()

	var instance *model.Instance
	var err error
	for {
		instance, err = s.TryGetIdleSlot()
		if err != nil {
			errorMessage := fmt.Sprintf("TryGetIdleSlot instance failed with: %s", err.Error())
			return nil, status.Errorf(codes.Internal, errorMessage)
		}
		if instance != nil {
			break
		}

		if constructDone {
			instance, err = s.TryGetIdleSlot()
			if instance != nil {
				break
			}
			errorMessage := fmt.Sprintf("ConstructAndPushSlotToQueue failed, request id: %s", request.RequestId)
			return nil, status.Errorf(codes.Internal, errorMessage)
		}

		time.Sleep(s.config.WaitTimeInitial)
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

	s.mu.Lock()
	s.durationTimes.AddOrUpdate(request.Assigment.RequestId, sortedset.SCORE(request.Result.DurationInMs), "")
	s.mu.Unlock()

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
			if s.expectSize() <= len(s.instances) || s.durationTimes.GetCount() == 0 {
				continue
			}
			s.mu.Lock()
			if element := s.idleInstance.Back(); element != nil {
				instance := element.Value.(*model.Instance)
				idleDuration := time.Now().Sub(instance.LastIdleTime)
				//D := s.deltaTimes.GetByRank(s.deltaTimes.GetCount()/2, false).Score()
				Q := s.durationTimes.GetByRank(s.durationTimes.GetCount()/2, false).Score()
				if sortedset.SCORE(idleDuration.Milliseconds()) > 2*Q {
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
