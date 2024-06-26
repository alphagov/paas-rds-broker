// Copyright (C) 2015-Present Pivotal Software, Inc. All rights reserved.

// This program and the accompanying materials are made available under
// the terms of the under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package brokerapi

import (
	"net/http"

	"code.cloudfoundry.org/lager/v3"
	"github.com/gorilla/mux"
	"github.com/pivotal-cf/brokerapi/v9/handlers"
)

type BrokerCredentials struct {
	Username string
	Password string
}

func New(serviceBroker ServiceBroker, logger lager.Logger, brokerCredentials BrokerCredentials) http.Handler {
	return NewWithOptions(serviceBroker, logger, WithBrokerCredentials(brokerCredentials))
}

func NewWithCustomAuth(serviceBroker ServiceBroker, logger lager.Logger, authMiddleware mux.MiddlewareFunc) http.Handler {
	return NewWithOptions(serviceBroker, logger, WithCustomAuth(authMiddleware))
}

func AttachRoutes(router *mux.Router, serviceBroker ServiceBroker, logger lager.Logger) {
	attachRoutes(router, serviceBroker, logger)
}

func attachRoutes(router *mux.Router, serviceBroker ServiceBroker, logger lager.Logger) {
	apiHandler := handlers.NewApiHandler(serviceBroker, logger)
	router.HandleFunc("/v2/catalog", apiHandler.Catalog).Methods("GET")

	router.HandleFunc("/v2/service_instances/{instance_id}", apiHandler.GetInstance).Methods("GET")
	router.HandleFunc("/v2/service_instances/{instance_id}", apiHandler.Provision).Methods("PUT")
	router.HandleFunc("/v2/service_instances/{instance_id}", apiHandler.Deprovision).Methods("DELETE")
	router.HandleFunc("/v2/service_instances/{instance_id}/last_operation", apiHandler.LastOperation).Methods("GET")
	router.HandleFunc("/v2/service_instances/{instance_id}", apiHandler.Update).Methods("PATCH")

	router.HandleFunc("/v2/service_instances/{instance_id}/service_bindings/{binding_id}", apiHandler.GetBinding).Methods("GET")
	router.HandleFunc("/v2/service_instances/{instance_id}/service_bindings/{binding_id}", apiHandler.Bind).Methods("PUT")
	router.HandleFunc("/v2/service_instances/{instance_id}/service_bindings/{binding_id}", apiHandler.Unbind).Methods("DELETE")

	router.HandleFunc("/v2/service_instances/{instance_id}/service_bindings/{binding_id}/last_operation", apiHandler.LastBindingOperation).Methods("GET")
}
