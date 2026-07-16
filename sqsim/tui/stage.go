// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tui

import "github.com/uber/submitqueue/sqsim/runner"

type stage struct {
	statuses map[string]struct{}
}

var requestStages = []stage{
	{statuses: statusSet("validating", "validated")},
	{statuses: statusSet("batching", "batched")},
	{statuses: statusSet("scored")},
	{statuses: statusSet("speculating", "speculated", "waitingpath")},
	{statuses: statusSet("building", "built")},
	{statuses: statusSet("landing", "landed")},
}

func stageCells(request runner.Request, spinner string) [6]string {
	current := -1
	reached := make([]bool, len(requestStages))
	for _, event := range request.History {
		if index := stageIndex(event.Status); index >= 0 {
			current = index
			reached[index] = true
		}
	}
	if index := stageIndex(request.Status); index >= 0 {
		current = index
		reached[index] = true
	}

	var cells [6]string
	for i := range requestStages {
		switch {
		case request.Status == "error" && i == current:
			cells[i] = "x"
		case request.Status == "cancelled" && i == current:
			cells[i] = "x"
		case i < current || (i == current && stageComplete(i, request.Status)):
			cells[i] = "ok"
		case i == current:
			if request.Status == "building" {
				cells[i] = spinner
			} else {
				cells[i] = ">"
			}
		case reached[i]:
			cells[i] = "ok"
		default:
			cells[i] = "."
		}
	}
	return cells
}

func stageComplete(index int, status string) bool {
	switch index {
	case 0:
		return status == "validated" || stageIndex(status) > index
	case 1:
		return status == "batched" || stageIndex(status) > index
	case 2:
		return status == "scored" || stageIndex(status) > index
	case 3:
		return status == "speculated" || status == "waitingpath" || stageIndex(status) > index
	case 4:
		return status == "built" || stageIndex(status) > index
	case 5:
		return status == "landed"
	default:
		return false
	}
}

func stageIndex(status string) int {
	for i, stage := range requestStages {
		if _, ok := stage.statuses[status]; ok {
			return i
		}
	}
	return -1
}

func statusSet(statuses ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(statuses))
	for _, status := range statuses {
		set[status] = struct{}{}
	}
	return set
}
