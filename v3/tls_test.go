/*
 * Copyright (c) 2021 AccelByte Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package eventstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testCertFile       = "tls_cert_test.crt"
	testPrivateKeyFile = "tls_private_key_test.pem"

	errorGetTLSCert = "error getting TLS cert"
)

// nolint dupl
func TestGetTLSCertFromCertFileSuccess(t *testing.T) {
	cert, err := GetTLSCertFromFile(testCertFile)
	if err != nil {
		assert.FailNow(t, errorGetTLSCert, err)
		return
	}

	assert.NotNil(t, cert.Certificate, "certificate should not be nil")
	assert.Nil(t, cert.PrivateKey, "private key should be nil")
}

// nolint dupl
func TestGetTLSCertFromPrivateKeyFileSuccess(t *testing.T) {
	cert, err := GetTLSCertFromFile(testPrivateKeyFile)
	if err != nil {
		assert.FailNow(t, errorGetTLSCert, err)
		return
	}

	assert.NotNil(t, cert.PrivateKey, "private key should not be nil")
	assert.Nil(t, cert.Certificate, "certificate should be nil")
}
