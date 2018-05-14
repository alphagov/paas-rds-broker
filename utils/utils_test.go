package utils_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/alphagov/paas-rds-broker/utils"
)

var _ = Describe("RandomAlphaNum", func() {
	It("generates a random alpha numeric with the proper length", func() {
		randomString := RandomAlphaNum(32)
		Expect(len(randomString)).To(Equal(32))
	})
})

var _ = Describe("GenerateHash", func() {
	It("returns the Base64 encoded SHA256 hash of the given string", func() {
		hash := GenerateHash("ce71b484-d542-40f7-9dd4-5526e38c81ba", 64)
		// Expectation generated with
		// echo -n "ce71b484-d542-40f7-9dd4-5526e38c81ba" | openssl dgst -sha256 -binary | openssl enc -base64
		Expect(hash).To(Equal("BJ3IzLRK6pmhB98A1S7RmgWkkgmK1MSQgKUMikmI7yQ="))
	})

	It("truncates the result when it's longer than the resuested max size", func() {
		hash := GenerateHash("ce71b484-d542-40f7-9dd4-5526e38c81ba", 32)
		Expect(hash).To(Equal("BJ3IzLRK6pmhB98A1S7RmgWkkgmK1MSQ"))
	})

	It("Uses the URL safe base64 scheme", func() {
		hash := GenerateHash("1123456678", 64)
		// Expectation generated with
		// echo -n 1123456678 | openssl dgst -sha256 -binary | openssl enc -base64 | tr '+/' '-_'
		Expect(hash).To(Equal("180NZHG1Cx3N4mkrcboPAzOeJlXlYth4mKxjtzGhXMI="))
	})
})
