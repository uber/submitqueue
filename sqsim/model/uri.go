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

package model

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

const (
	uriScheme = "sqsim"
	uriHost   = "local"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// Reference identifies one Land in one named scenario.
type Reference struct {
	// Scenario is the public scenario registry name.
	Scenario string
	// Land is the Land name within the scenario.
	Land string
}

// ChangeURI returns the synthetic change URI for one scenario Land.
func ChangeURI(scenario, land string) (string, error) {
	if err := validateName("scenario", scenario); err != nil {
		return "", err
	}
	if err := validateName("land", land); err != nil {
		return "", err
	}
	return (&url.URL{
		Scheme: uriScheme,
		Host:   uriHost,
		Path:   "/" + scenario + "/" + land,
	}).String(), nil
}

// ParseChangeURI parses an SQSim synthetic change URI.
func ParseChangeURI(raw string) (Reference, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return Reference{}, fmt.Errorf("parse change URI: %w", err)
	}
	if parsed.Scheme != uriScheme || parsed.Host != uriHost || parsed.User != nil {
		return Reference{}, fmt.Errorf("change URI must use %s://%s", uriScheme, uriHost)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return Reference{}, fmt.Errorf("change URI must not contain a query or fragment")
	}
	segments := strings.Split(strings.TrimPrefix(parsed.EscapedPath(), "/"), "/")
	if len(segments) != 2 {
		return Reference{}, fmt.Errorf("change URI path must contain scenario and land")
	}
	scenario, err := url.PathUnescape(segments[0])
	if err != nil {
		return Reference{}, fmt.Errorf("decode scenario: %w", err)
	}
	land, err := url.PathUnescape(segments[1])
	if err != nil {
		return Reference{}, fmt.Errorf("decode land: %w", err)
	}
	if err := validateName("scenario", scenario); err != nil {
		return Reference{}, err
	}
	if err := validateName("land", land); err != nil {
		return Reference{}, err
	}
	return Reference{Scenario: scenario, Land: land}, nil
}

func validateName(kind, name string) error {
	if !namePattern.MatchString(name) {
		return fmt.Errorf("%s name %q must be one URI-safe path segment", kind, name)
	}
	return nil
}
