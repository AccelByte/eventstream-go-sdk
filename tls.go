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
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"

	"github.com/sirupsen/logrus"
)

const (
	DefaultSSLCertPath = "/etc/ssl/certs/ca-certificates.crt" // Alpine certificate path

	certificateBlockType = "CERTIFICATE"
)

// GetTLSCertFromFile reads file, divides into key and certificates
func GetTLSCertFromFile(path string) (*tls.Certificate, error) {
	raw, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cert tls.Certificate

	for {
		block, rest := pem.Decode(raw)
		if block == nil {
			break
		}

		if block.Type == certificateBlockType {
			cert.Certificate = append(cert.Certificate, block.Bytes)
		} else {
			cert.PrivateKey, err = parsePrivateKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("failure reading private key from %s: %s", path, err.Error())
			}
		}

		raw = rest
	}

	if len(cert.Certificate) == 0 && cert.PrivateKey == nil {
		return nil, fmt.Errorf("no certificate or private key found in \"%s\"", path)
	}

	return &cert, nil
}

func parsePrivateKey(der []byte) (crypto.PrivateKey, error) {
	logrus.Debug("get private key")

	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}

	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		switch key := key.(type) {
		case *rsa.PrivateKey, *ecdsa.PrivateKey:
			return key, nil
		default:
			return nil, fmt.Errorf("found unknown private key type in PKCSS8 wrapping")
		}
	}

	key, err := x509.ParseECPrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %s", err.Error())
	}

	return key, nil
}
